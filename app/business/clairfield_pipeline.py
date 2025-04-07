import json
import csv
import asyncio
from app.db.session import SessionLocal
from app.db.models import Article
from app.utils.logger import logger
from crewai import Crew, Task
from app.business.agents import create_deal_extractor_agent
from crawl4ai import AsyncWebCrawler


async def crawl_url(url):
    """Use crawl4ai to fetch and convert article to markdown"""
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url)
            return result.markdown if hasattr(result, 'markdown') else str(result)
    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return ''


def run_pipeline():
    db = SessionLocal()
    extractor_agent = create_deal_extractor_agent()

    articles = db.query(Article).filter(Article.link.ilike('%www.clairfield.com%')).all()
    logger.info(f"🔍 Found {len(articles)} Clairfield articles")

    output_data = []

    # ✅ Prepare CSV writer
    csv_file = open("clairfield_deal_data.csv", "w", newline="", encoding="utf-8")
    fieldnames = [
        "article_title", "article_link", "buyer", "seller", "company",
        "investor", "divestor", "date", "amount", "countries_involved", "additional_notes"
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for idx, article in enumerate(articles, start=1):
        print(f"\n🔄 Processing article {idx}/{len(articles)}: {article.title}")
        url = article.link

        try:
            markdown_content = asyncio.run(crawl_url(url))
        except Exception as e:
            logger.error(f"❌ Crawling failed: {e}")
            markdown_content = ""

        if not markdown_content:
            logger.warning(f"⚠️ No content found for {url}")
            continue

        full_text = f"{article.title}\n\n{article.summary}\n\n{markdown_content}"

        # ✅ Updated extraction prompt
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
            data["article_link"] = article.link
            data["article_title"] = article.title
            output_data.append(data)

            # Write to CSV
            writer.writerow({
                "article_title": article.title,
                "article_link": article.link,
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

            logger.info(f"✅ Deal extracted for: {article.title}")
        except Exception as e:
            logger.warning(f"❌ Failed to parse extraction result: {result} - {e}")

        # Mark article as processed
        article.processed = True
        article.is_deal_related = True
        db.commit()

    # Save to JSON
    with open("clairfield_deal_data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    csv_file.close()
    logger.info("📁 Saved output to clairfield_deal_data.json and clairfield_deal_data_1.csv")
    db.close()


if __name__ == '__main__':
    run_pipeline()