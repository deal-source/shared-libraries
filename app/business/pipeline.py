import json
import csv
import asyncio
import datetime
import logging
from crewai import Crew, Task
from app.business.agents import create_deal_extractor_agent, create_merger_agent
from crawl4ai import AsyncWebCrawler

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def crawl_url(crawler, url):
    """Fetch markdown content using Crawl4AI"""
    try:
        result = await crawler.arun(url=url)  # Add browser=True if needed
        return result.markdown if hasattr(result, 'markdown') else str(result)
    except Exception as e:
        logger.error(f"❌ Error crawling {url}: {e}")
        return ''


def extract_article_title(content):
    lines = content.strip().split('\n')
    for line in lines:
        if line.strip():
            return line.strip()[:100]
    return "Untitled Article"


def check_deal_relevance(title, content):
    merger_agent = create_merger_agent()
    task = Task(
        description=(
            "Determine if the following article is related to mergers, acquisitions, or financial deals. "
            "Return 'YES' if it's deal-related, or 'NO' if it's not.\n\n"
            f"Title: {title}\nContent: {content[:1000]}"
        ),
        expected_output="YES or NO",
        agent=merger_agent
    )
    crew = Crew(agents=[merger_agent], tasks=[task])
    result = crew.kickoff()
    return "YES" in result.tasks_output[0].raw.upper()


async def process_article(crawler, url, writer, extractor_agent):
    markdown_content = await crawl_url(crawler, url)
    if not markdown_content:
        writer.writerow({
            "article_title": "Unknown",
            "article_link": url,
            "is_deal_related": "No",
            "buyer": "", "seller": "", "company": "", "investor": "", "divestor": "",
            "date": "", "amount": "", "countries_involved": "", "additional_notes": "Failed to crawl"
        })
        return None

    title = extract_article_title(markdown_content)
    is_deal_related = check_deal_relevance(title, markdown_content)

    if not is_deal_related:
        logger.info(f"⏭️ Skipping non-deal article: {url}")
        writer.writerow({
            "article_title": title,
            "article_link": url,
            "is_deal_related": "No",
            "buyer": "", "seller": "", "company": "", "investor": "", "divestor": "",
            "date": "", "amount": "", "countries_involved": "", "additional_notes": "Not deal related"
        })
        return None

    logger.info(f"✅ Deal-related article: {url}")
    full_text = f"{title}\n\n{markdown_content}"

    extract_task = Task(
        description=(
            "Extract detailed deal information from the article below. "
            "Return a JSON object with keys: buyer, seller, company, investor, divestor, date, amount, countries_involved, additional_notes\n\n"
            f"Article:\n{full_text}"
        ),
        expected_output="JSON with relevant keys",
        agent=extractor_agent
    )

    extract_crew = Crew(agents=[extractor_agent], tasks=[extract_task])
    result = extract_crew.kickoff()

    try:
        data = json.loads(result.tasks_output[0].raw)
        data.update({
            "article_title": title,
            "article_link": url,
            "is_deal_related": "Yes"
        })
        writer.writerow({
            **data,
            "buyer": data.get("buyer", ""),
            "seller": data.get("seller", ""),
            "company": data.get("company", ""),
            "investor": data.get("investor", ""),
            "divestor": data.get("divestor", ""),
            "date": data.get("date", ""),
            "amount": data.get("amount", ""),
            "countries_involved": data.get("countries_involved", ""),
            "additional_notes": data.get("additional_notes", "")
        })
        return data
    except Exception as e:
        logger.warning(f"❌ JSON parse failed: {e}")
        writer.writerow({
            "article_title": title,
            "article_link": url,
            "is_deal_related": "Yes",
            "buyer": "", "seller": "", "company": "", "investor": "", "divestor": "",
            "date": "", "amount": "", "countries_involved": "", "additional_notes": "Parse failure"
        })
        return None


async def run_pipeline_async():
    INPUT_CSV = "input_urls.csv"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    extractor_agent = create_deal_extractor_agent()
    output_data = []

    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if 'url' not in reader.fieldnames:
                logger.error("CSV missing 'url' header")
                return
            articles = [row['url'].strip() for row in reader if row['url'].strip()]
    except Exception as e:
        logger.error(f"Error reading input: {e}")
        return

    logger.info(f"🔍 Found {len(articles)} URLs")

    csv_filename = f"deal_data_{timestamp}.csv"
    json_filename = f"deal_data_{timestamp}.json"
    fieldnames = ["article_title", "article_link", "buyer", "seller", "company",
                  "investor", "divestor", "date", "amount", "countries_involved",
                  "additional_notes", "is_deal_related"]

    with open(csv_filename, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        async with AsyncWebCrawler() as crawler:
            for idx, url in enumerate(articles, 1):
                logger.info(f"\n🔄 Processing {idx}/{len(articles)}: {url}")
                data = await process_article(crawler, url, writer, extractor_agent)
                if data:
                    output_data.append(data)

    with open(json_filename, "w", encoding="utf-8") as json_file:
        json.dump(output_data, json_file, ensure_ascii=False, indent=2)

    logger.info(f"📁 Results saved to {csv_filename} and {json_filename}")


if __name__ == "__main__":
    asyncio.run(run_pipeline_async())