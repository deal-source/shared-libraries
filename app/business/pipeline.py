import json
import csv
import asyncio
from bs4 import BeautifulSoup  # <== We will use this to extract page title
from crewai import Crew, Task
from app.utils.logger import logger
from app.business.agents import create_deal_extractor_agent
from crawl4ai import AsyncWebCrawler

INPUT_CSV = "input_urls.csv"  # Your input CSV with only 'url' header
OUTPUT_CSV = "clairfield_deal_data.csv"
OUTPUT_JSON = "clairfield_deal_data.json"


async def crawl_url(url):
    """Use crawl4ai to fetch and convert article to markdown, also fetch raw html"""
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url)
            return {
                "markdown": result.markdown if hasattr(result, 'markdown') else str(result),
                "html": result.html if hasattr(result, 'html') else ""
            }
    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return {"markdown": "", "html": ""}


def extract_title_from_html(html_content):
    """Simple function to extract <title> from raw HTML"""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        return soup.title.string.strip() if soup.title and soup.title.string else ""
    except Exception as e:
        logger.warning(f"Failed to extract title from HTML: {e}")
        return ""


def run_pipeline():
    extractor_agent = create_deal_extractor_agent()

    # ✅ Read URLs from CSV
    urls = []
    with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            url = row.get("url", "").strip()
            if url:
                urls.append(url)

    logger.info(f"🔍 Found {len(urls)} URLs to process")

    output_data = []

    # ✅ Prepare CSV writer
    csv_file = open(OUTPUT_CSV, "w", newline='', encoding='utf-8')
    fieldnames = [
        "article_title", "article_link", "buyer", "seller", "company",
        "investor", "divestor", "date", "amount", "countries_involved", "additional_notes"
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for idx, url in enumerate(urls, start=1):
        print(f"\n🔄 Processing article {idx}/{len(urls)}: {url}")

        try:
            crawl_result = asyncio.run(crawl_url(url))
            markdown_content = crawl_result.get("markdown", "")
            html_content = crawl_result.get("html", "")
        except Exception as e:
            logger.error(f"❌ Crawling failed: {e}")
            markdown_content = ""
            html_content = ""

        if not markdown_content:
            logger.warning(f"⚠️ No content found for {url}")
            continue

        # 🏷️ Extract title from HTML
        title = extract_title_from_html(html_content)
        if not title:
            title = "Unknown Title"

        full_text = f"{title}\n\n{markdown_content}"

        extract_task = Task(
            description=(
                "Extract detailed deal information from the article below. "
                "Return a JSON object with the following keys:\n"
                "`buyer`, `seller`, `company`, `investor`, `divestor`, `date`, `amount`, `countries_involved`, `additional_notes`\n\n"
                f"Article:\n{full_text}"
            ),
            expected_output=(
                "JSON with keys: buyer, seller, company, investor, divestor, date, amount, countries_involved, additional_notes"
            ),
            agent=extractor_agent,
        )

        extract_crew = Crew(agents=[extractor_agent], tasks=[extract_task])
        result = extract_crew.kickoff()

        try:
            data = json.loads(result.tasks_output[0].raw)
            # Add base info
            data["article_link"] = url
            data["article_title"] = title
            output_data.append(data)

            # Write to CSV
            writer.writerow({
                "article_title": title,
                "article_link": url,
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

            logger.info(f"✅ Deal extracted for: {title}")
        except Exception as e:
            logger.warning(f"❌ Failed to parse extraction result: {result} - {e}")

    # Save to JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    csv_file.close()
    logger.info(f"📁 Saved output to {OUTPUT_JSON} and {OUTPUT_CSV}")


if __name__ == '__main__':
    run_pipeline()