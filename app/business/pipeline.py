# pipeline.py
import json
import csv
import asyncio
import datetime
import logging
import random
import time
import re
import os
from bs4 import BeautifulSoup
from crewai import Crew, Task, Process
from app.business.agents import (
    create_deal_extractor_agent,
    create_merger_agent,
    create_website_lookup_agent,
    create_database_writer_agent
)
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Rate limiting configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 10  # Base delay in seconds
RANDOM_DELAY_MIN = 5  # Minimum random delay between requests
RANDOM_DELAY_MAX = 15  # Maximum random delay between requests


async def fetch_content_with_playwright(url, retry_count=0):
    """Fetch content using Playwright with rate limiting and retries"""
    # Add random delay to avoid rate limiting
    delay = random.uniform(3, 8)
    await asyncio.sleep(delay)

    try:
        async with async_playwright() as p:
            # Random browser selection for less fingerprinting
            browsers = [p.chromium, p.firefox, p.webkit]
            browser_engine = random.choice(browsers)

            # Launch browser with various fingerprint-changing options
            browser = await browser_engine.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials'
                ]
            )

            # Create a context with randomized viewport and user agent
            context = await browser.new_context(
                viewport={'width': random.randint(1024, 1920), 'height': random.randint(768, 1080)},
                user_agent=f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 110)}.0.{random.randint(1000, 9999)}.{random.randint(100, 999)} Safari/537.36"
            )

            # Create a new page and navigate to the URL
            page = await context.new_page()

            # Set timeout to handle slow-loading pages
            page.set_default_timeout(60000)

            # Add header to appear more like a real browser
            await page.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })

            response = await page.goto(url, wait_until='networkidle')

            # Check if we got blocked
            if response.status == 429 or response.status == 403:
                if retry_count < MAX_RETRIES:
                    logger.warning(
                        f"⚠️ Rate limited on {url}, retrying in a moment (attempt {retry_count + 1}/{MAX_RETRIES})")
                    await browser.close()
                    # Exponential backoff with jitter
                    retry_delay = RETRY_DELAY_BASE * (2 ** retry_count) + random.uniform(10, 30)
                    await asyncio.sleep(retry_delay)
                    return await fetch_content_with_playwright(url, retry_count + 1)
                else:
                    logger.error(f"❌ Failed to access {url} after multiple retries")
                    await browser.close()
                    return ""

            # Wait for a bit to ensure JavaScript execution
            await asyncio.sleep(2)

            # Extract the page content
            html_content = await page.content()

            # Extract the readable text content for processing
            text_content = await page.evaluate("""() => {
                return document.body.innerText;
            }""")

            # Take a screenshot for debugging
            screenshot_dir = "debug_screenshots"
            os.makedirs(screenshot_dir, exist_ok=True)
            safe_url = url.replace("://", "_").replace("/", "_").replace(".", "_")[:50]
            screenshot_path = f"{screenshot_dir}/{safe_url}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            await page.screenshot(path=screenshot_path)

            # Close the browser
            await browser.close()

            # Convert HTML to a more readable format using BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract title
            title = soup.title.string if soup.title else "Untitled"

            # Remove script and style tags
            for script in soup(["script", "style"]):
                script.decompose()

            # Get the main content
            main_content = soup.get_text(separator="\n", strip=True)

            # Format into markdown-like structure
            markdown_content = f"# {title}\n\n{main_content}"

            return markdown_content

    except Exception as e:
        logger.error(f"❌ Error fetching {url}: {e}")
        if retry_count < MAX_RETRIES:
            # Exponential backoff with jitter
            retry_delay = 30 * (2 ** retry_count) + random.uniform(5, 15)
            logger.warning(f"⚠️ Retrying in {retry_delay:.1f} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
            await asyncio.sleep(retry_delay)
            return await fetch_content_with_playwright(url, retry_count + 1)
        return ""


def extract_article_title(content):
    """Extract the title from the markdown content"""
    if not content:
        return "Unknown Article"

    # Handle error pages specifically
    if "Error 429" in content or "too many requests" in content:
        return "Error 429: Rate Limited"

    # Extract from markdown heading
    lines = content.strip().split('\n')
    for line in lines:
        if line.strip().startswith("# "):
            return line.replace("# ", "").strip()[:100]

    # Fallback to first non-empty line
    for line in lines:
        if line.strip():
            return line.strip()[:100]

    return "Untitled Article"


def check_deal_relevance(title, content):
    """Check if the article is related to financial deals"""
    # Don't process rate-limited content
    if "Error 429" in title or "too many requests" in content:
        return False

    merger_agent = create_merger_agent()
    task = Task(
        description=(
            "Determine if the following article relates to any financial transaction or deal including:\n"
            "- Mergers and acquisitions\n"
            "- Private equity investments\n"
            "- Venture capital funding\n"
            "- Debt financing\n"
            "- Public offerings\n"
            "- Minority stake purchases\n"
            "- Asset sales or divestitures\n\n"
            "Return ONLY 'YES' if it's deal-related, or 'NO' if it's not.\n\n"
            f"Title: {title}\nContent: {content[:1500]}"
        ),
        expected_output="YES or NO",
        agent=merger_agent
    )
    crew = Crew(agents=[merger_agent], tasks=[task])
    result = crew.kickoff()
    return "YES" in result.tasks_output[0].raw.upper()


def enrich_deal_data_with_websites(data):
    """
    Directly enrich deal data with company websites using the Forager API.
    This is a non-CrewAI approach as a fallback.
    """
    from app.business.agents import CompanyWebsiteTool

    enriched_data = data.copy()
    website_tool = CompanyWebsiteTool()

    # Process each company field
    company_fields = ["buyer", "seller", "company", "investor", "divestor", "target"]

    for field in company_fields:
        if field in data and data[field]:
            company_name = data[field]
            website_field = f"{field}_website"

            logger.info(f"Looking up website for {field}: {company_name}")
            website = website_tool._run(company_name)

            if website != "Not found" and website != "No company name provided":
                enriched_data[website_field] = website
                logger.info(f"Found website for {field}: {website}")

    return enriched_data


def enrich_with_company_websites(data, website_agent):
    """Enrich deal data with company websites using the website lookup agent"""

    # Create list of company entities to process
    entities = []

    # Extract company names that need website lookup
    if data.get("buyer"):
        entities.append(("buyer", data["buyer"]))
    if data.get("seller"):
        entities.append(("seller", data["seller"]))
    if data.get("company"):
        entities.append(("company", data["company"]))
    if data.get("investor"):
        entities.append(("investor", data["investor"]))
    if data.get("divestor"):
        entities.append(("divestor", data["divestor"]))
    if data.get("target"):
        entities.append(("target", data["target"]))

    # If no entities to process, return original data
    if not entities:
        logger.info("No company entities found for website lookup")
        return data

    # Format entities for lookup
    entity_list = "\n".join([f"- {entity_type}: {name}" for entity_type, name in entities])

    # Create the enrichment task
    enrichment_task = Task(
        description=(
            "For each company name listed below, use the fetch_company_website tool to find its official website domain.\n\n"
            f"Companies:\n{entity_list}\n\n"
            "Return a JSON object mapping each company type to its website domain. Format:\n"
            "{\n"
            '  "[entity_type]_website": "domain.com",\n'
            '  ...\n'
            "}\n\n"
            "Only include entries where a website is successfully found."
        ),
        expected_output="JSON with company website mappings",
        agent=website_agent
    )

    # Create and run the crew
    enrichment_crew = Crew(
        agents=[website_agent],
        tasks=[enrichment_task],
        process=Process.sequential
    )

    try:
        result = enrichment_crew.kickoff()

        # Parse the result
        raw_output = result.tasks_output[0].raw

        # Clean the JSON string if it has markdown formatting
        if "```json" in raw_output:
            json_str = raw_output.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_output:
            json_str = raw_output.split("```")[1].split("```")[0].strip()
        else:
            json_str = raw_output.strip()

        website_data = json.loads(json_str)
        logger.info(f"✅ Found {len(website_data)} company websites")

        # Update the original data with website information
        enriched_data = data.copy()
        enriched_data.update(website_data)

        return enriched_data
    except Exception as e:
        logger.error(f"❌ Failed to parse website data: {e}")
        # Fallback to direct method if CrewAI method fails
        logger.info("Falling back to direct website lookup method")
        return enrich_deal_data_with_websites(data)


class StatusManager:
    """Manager for handling URL processing statuses"""

    def __init__(self, input_csv_path):
        self.input_csv_path = input_csv_path
        self.url_statuses = self._load_statuses()
        self.lock = asyncio.Lock()

    def _load_statuses(self):
        """Load the current statuses from the CSV file"""
        url_statuses = {}

        # Check if file exists
        if not os.path.exists(self.input_csv_path):
            # Create new file with headers
            with open(self.input_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['url', 'status', 'notes'])
            return url_statuses

        with open(self.input_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Check if status column exists
            if 'status' not in reader.fieldnames:
                # Create a temporary copy with status column
                temp_path = f"{self.input_csv_path}.temp"
                with open(temp_path, 'w', newline='', encoding='utf-8') as temp_f:
                    fieldnames = list(reader.fieldnames) + ['status', 'notes']
                    writer = csv.DictWriter(temp_f, fieldnames=fieldnames)
                    writer.writeheader()

                    # Reset reader to start
                    f.seek(0)
                    next(reader)  # Skip header

                    for row in reader:
                        row['status'] = ''  # Initialize as empty
                        row['notes'] = ''
                        writer.writerow(row)

                # Replace original file
                os.replace(temp_path, self.input_csv_path)

                # Load with new structure
                with open(self.input_csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url_statuses[row['url']] = {
                            'status': row.get('status', ''),
                            'notes': row.get('notes', '')
                        }
            else:
                # Status column already exists
                for row in reader:
                    url_statuses[row['url']] = {
                        'status': row.get('status', ''),
                        'notes': row.get('notes', '')
                    }

        return url_statuses

    async def update_status(self, url, status, notes=''):
        """Update the status for a URL and write to the CSV file"""
        async with self.lock:
            # Update in memory
            self.url_statuses[url] = {
                'status': status,
                'notes': notes
            }

            # Write to CSV
            rows = []
            existing_urls = set()

            # Read existing data
            with open(self.input_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames

                for row in reader:
                    if row['url'] == url:
                        row['status'] = status
                        row['notes'] = notes
                    rows.append(row)
                    existing_urls.add(row['url'])

            # If URL doesn't exist, add it
            if url not in existing_urls:
                rows.append({
                    'url': url,
                    'status': status,
                    'notes': notes
                })

            # Write back to file
            with open(self.input_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            logger.info(f"✅ Updated status for {url} to {status}")

    def get_urls_to_process(self):
        """Get URLs that need processing"""
        urls_to_process = []

        with open(self.input_csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row['url']
                status = row.get('status', '')

                # Process URLs that are not yet crawled or had errors
                if not status or status == 'error':
                    urls_to_process.append(url)

        return urls_to_process


class ResultWriter:
    """Handler for writing results to CSV and JSON files"""

    def __init__(self, csv_filename, json_filename, fieldnames):
        self.csv_filename = csv_filename
        self.json_filename = json_filename
        self.fieldnames = fieldnames
        self.lock = asyncio.Lock()
        self.results = []

        # Initialize CSV file with headers
        with open(self.csv_filename, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.fieldnames)
            writer.writeheader()

    async def write_result(self, result_data):
        """Write a single result to both CSV and JSON files"""
        async with self.lock:
            # Write to CSV
            with open(self.csv_filename, "a", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fieldnames)
                writer.writerow(result_data)

            # Add to results list
            if result_data.get("is_deal_related") == "Yes":
                self.results.append(result_data)

            # Write complete JSON file each time (to ensure we always have latest data)
            with open(self.json_filename, "w", encoding="utf-8") as json_file:
                json.dump(self.results, json_file, ensure_ascii=False, indent=2)

            logger.info(f"✅ Wrote result for {result_data.get('article_link')} to CSV and JSON")


async def process_single_url(url, result_writer, extractor_agent, website_agent, db_writer_agent, status_manager):
    """Process a single URL completely and update status before returning"""
    logger.info(f"🔄 Processing URL: {url}")

    # Update status to processing
    await status_manager.update_status(url, "processing", "URL is being processed")

    # Fetch content
    markdown_content = await fetch_content_with_playwright(url)
    title = extract_article_title(markdown_content)

    # Check for rate limiting or empty content
    if not markdown_content or "Error 429" in title or "too many requests" in markdown_content:
        result_data = {
            "article_title": title or "Rate Limited",
            "article_link": url,
            "is_deal_related": "Unknown",
            "deal_type": "",
            "announcement_date": "",
            "buyer": "",
            "buyer_website": "",
            "seller": "",
            "seller_website": "",
            "company": "",
            "company_website": "",
            "investor": "",
            "investor_website": "",
            "divestor": "",
            "divestor_website": "",
            "target": "",
            "target_website": "",
            "amount": "",
            "currency": "",
            "stake_percentage": "",
            "countries_involved": "",
            "advisors": "",
            "strategic_rationale": "",
            "additional_notes": "Rate limited or failed to crawl"
        }
        # Write result
        await result_writer.write_result(result_data)

        # Update status to error
        await status_manager.update_status(url, "error", "Rate limited or failed to crawl")
        return None

    # Check if deal-related
    is_deal_related = check_deal_relevance(title, markdown_content)

    if not is_deal_related:
        logger.info(f"⏭️ Not a deal-related article: {url}")
        result_data = {
            "article_title": title,
            "article_link": url,
            "is_deal_related": "No",
            "deal_type": "",
            "announcement_date": "",
            "buyer": "",
            "buyer_website": "",
            "seller": "",
            "seller_website": "",
            "company": "",
            "company_website": "",
            "investor": "",
            "investor_website": "",
            "divestor": "",
            "divestor_website": "",
            "target": "",
            "target_website": "",
            "amount": "",
            "currency": "",
            "stake_percentage": "",
            "countries_involved": "",
            "advisors": "",
            "strategic_rationale": "",
            "additional_notes": "Not deal related"
        }
        # Write result
        await result_writer.write_result(result_data)

        # Update status to no_deals
        await status_manager.update_status(url, "no_deals", "Not deal related")
        return None

    logger.info(f"✅ Deal-related article found: {url}")
    full_text = f"{title}\n\n{markdown_content}"

    # Extract deal data
    extract_task = Task(
        description=(
            "Extract comprehensive deal information from the article below. "
            "Return a JSON object with the following structure:\n\n"
            "{\n"
            '  "deal_type": "M&A, PE investment, VC funding, debt financing, etc.",\n'
            '  "announcement_date": "When deal was announced (YYYY-MM-DD if available)",\n'
            '  "buyer": "Name of acquiring/buying company/investor (if applicable)",\n'
            '  "seller": "Name of selling entity (if applicable)",\n'
            '  "investor": "Name of investor in funding round (if applicable)",\n'
            '  "divestor": "Entity divesting assets (if applicable)",\n'
            '  "company": "Main company involved if not clearly buyer/seller",\n'
            '  "target": "Entity being acquired/invested in",\n'
            '  "amount": "Numerical value of transaction",\n'
            '  "currency": "USD, EUR, etc.",\n'
            '  "stake_percentage": "Percentage stake acquired if mentioned",\n'
            '  "countries_involved": "Countries of entities or transaction jurisdiction",\n'
            '  "advisors": "Financial/legal advisors if mentioned",\n'
            '  "strategic_rationale": "Brief explanation of deal purpose",\n'
            '  "additional_notes": "Any other important details"\n'
            "}\n\n"
            "Only include fields where information is available in the article. "
            "Be precise and ensure all extracted data is formatted correctly as JSON.\n\n"
            f"Article:\n{full_text}"
        ),
        expected_output="JSON with deal information",
        agent=extractor_agent
    )

    extract_crew = Crew(agents=[extractor_agent], tasks=[extract_task])

    try:
        result = extract_crew.kickoff()

        # Handle potential JSON formatting issues
        raw_output = result.tasks_output[0].raw
        # Clean the JSON string if it has markdown formatting
        if "```json" in raw_output:
            json_str = raw_output.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_output:
            json_str = raw_output.split("```")[1].split("```")[0].strip()
        else:
            json_str = raw_output.strip()

        data = json.loads(json_str)
        data.update({
            "article_title": title,
            "article_link": url,
            "is_deal_related": "Yes"
        })

        # Enrich with company websites
        logger.info(f"🔍 Looking up company websites for {url}...")
        enriched_data = enrich_with_company_websites(data, website_agent)

        # Ensure all fields exist in CSV even if not in JSON
        result_data = {
            "article_title": title,
            "article_link": url,
            "is_deal_related": "Yes",
            "deal_type": enriched_data.get("deal_type", ""),
            "announcement_date": enriched_data.get("announcement_date", ""),
            "buyer": enriched_data.get("buyer", ""),
            "buyer_website": enriched_data.get("buyer_website", ""),
            "seller": enriched_data.get("seller", ""),
            "seller_website": enriched_data.get("seller_website", ""),
            "company": enriched_data.get("company", ""),
            "company_website": enriched_data.get("company_website", ""),
            "investor": enriched_data.get("investor", ""),
            "investor_website": enriched_data.get("investor_website", ""),
            "divestor": enriched_data.get("divestor", ""),
            "divestor_website": enriched_data.get("divestor_website", ""),
            "target": enriched_data.get("target", ""),
            "target_website": enriched_data.get("target_website", ""),
            "amount": enriched_data.get("amount", ""),
            "currency": enriched_data.get("currency", ""),
            "stake_percentage": enriched_data.get("stake_percentage", ""),
            "countries_involved": enriched_data.get("countries_involved", ""),
            "advisors": enriched_data.get("advisors", ""),
            "strategic_rationale": enriched_data.get("strategic_rationale", ""),
            "additional_notes": enriched_data.get("additional_notes", "")
        }

        # Write result
        await result_writer.write_result(result_data)

        # Write to database - add companies from the deal
        logger.info(f"💾 Writing company information to database...")

        # Add buyer to database
        if enriched_data.get("buyer"):
            buyer_name = enriched_data.get("buyer")
            buyer_website = enriched_data.get("buyer_website", "")
            db_writer_agent.tools[0]._run(buyer_name, buyer_website)
            logger.info(f"✅ Added/updated buyer in database: {buyer_name}")

        # Add seller to database
        if enriched_data.get("seller"):
            seller_name = enriched_data.get("seller")
            seller_website = enriched_data.get("seller_website", "")
            db_writer_agent.tools[0]._run(seller_name, seller_website)
            logger.info(f"✅ Added/updated seller in database: {seller_name}")

        # Add company to database (if not buyer/seller)
        if enriched_data.get("company"):
            company_name = enriched_data.get("company")
            company_website = enriched_data.get("company_website", "")
            db_writer_agent.tools[0]._run(company_name, company_website)
            logger.info(f"✅ Added/updated company in database: {company_name}")

        # Add target to database
        if enriched_data.get("target"):
            target_name = enriched_data.get("target")
            target_website = enriched_data.get("target_website", "")
            db_writer_agent.tools[0]._run(target_name, target_website)
            logger.info(f"✅ Added/updated target in database: {target_name}")

        # Add investor to database
        if enriched_data.get("investor"):
            investor_name = enriched_data.get("investor")
            investor_website = enriched_data.get("investor_website", "")
            db_writer_agent.tools[0]._run(investor_name, investor_website)
            logger.info(f"✅ Added/updated investor in database: {investor_name}")

        # Add divestor to database
        if enriched_data.get("divestor"):
            divestor_name = enriched_data.get("divestor")
            divestor_website = enriched_data.get("divestor_website", "")
            db_writer_agent.tools[0]._run(divestor_name, divestor_website)
            logger.info(f"✅ Added/updated divestor in database: {divestor_name}")

        # Update status to crawled
        await status_manager.update_status(url, "crawled", "Successfully processed")

        return enriched_data
    except Exception as e:
        logger.warning(f"❌ JSON parse failed for {url}: {e}\nRaw output: {result.tasks_output[0].raw[:500]}...")
        result_data = {
            "article_title": title,
            "article_link": url,
            "is_deal_related": "Yes",
            "deal_type": "",
            "announcement_date": "",
            "buyer": "",
            "buyer_website": "",
            "seller": "",
            "seller_website": "",
            "company": "",
            "company_website": "",
            "investor": "",
            "investor_website": "",
            "divestor": "",
            "divestor_website": "",
            "target": "",
            "target_website": "",
            "amount": "",
            "currency": "",
            "stake_percentage": "",
            "countries_involved": "",
            "advisors": "",
            "strategic_rationale": "",
            "additional_notes": f"Parse failure: {str(e)[:100]}"
        }

        # Write result
        await result_writer.write_result(result_data)

        # Update status to error
        await status_manager.update_status(url, "error", f"Parse failure: {str(e)[:100]}")

        return None


async def run_pipeline_async():
    """Main pipeline function that processes each URL one by one with proper status updates"""
    INPUT_CSV = "input_urls.csv"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Initialize agents
    extractor_agent = create_deal_extractor_agent()
    website_agent = create_website_lookup_agent()
    db_writer_agent = create_database_writer_agent()

    # Initialize status manager
    status_manager = StatusManager(INPUT_CSV)

    # Get URLs that need processing
    urls_to_process = status_manager.get_urls_to_process()

    if not urls_to_process:
        logger.info("No URLs to process. All URLs have been successfully crawled.")
        return

    logger.info(f"🔍 Found {len(urls_to_process)} URLs to process")

    # Create debug screenshots directory if it doesn't exist
    os.makedirs("debug_screenshots", exist_ok=True)

    # Define output filenames
    csv_filename = f"deal_data_{timestamp}.csv"
    json_filename = f"deal_data_{timestamp}.json"
    fieldnames = [
        "article_title", "article_link", "is_deal_related",
        "deal_type", "announcement_date",
        "buyer", "buyer_website",
        "seller", "seller_website",
        "company", "company_website",
        "investor", "investor_website",
        "divestor", "divestor_website",
        "target", "target_website",
        "amount", "currency", "stake_percentage",
        "countries_involved", "advisors", "strategic_rationale",
        "additional_notes"
    ]

    # Initialize result writer
    result_writer = ResultWriter(csv_filename, json_filename, fieldnames)

    # Process URLs one by one
    for url in urls_to_process:
        await process_single_url(url, result_writer, extractor_agent, website_agent, db_writer_agent, status_manager)

        # Add random delay between URLs
        delay = random.uniform(10, 20)
        logger.info(f"⏱️ Waiting {delay:.1f} seconds before processing next URL")
        await asyncio.sleep(delay)

    logger.info(f"🎉 Finished processing all URLs. Results saved to {csv_filename} and {json_filename}")


if __name__ == "__main__":
    asyncio.run(run_pipeline_async())