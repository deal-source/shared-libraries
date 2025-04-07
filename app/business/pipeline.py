from app.db.session import SessionLocal
from app.db.models import Article, Deal
from app.utils.logger import logger
from crewai import Crew, Task
from app.business.agents import create_merger_agent, create_deal_extractor_agent, create_crawler_agent
import json
import requests
from crawl4ai import AsyncWebCrawler
import asyncio


async def crawl_url(url):
    """Use crawl4ai to fetch and convert article to markdown"""
    try:
        # Create an instance of AsyncWebCrawler using async context manager
        async with AsyncWebCrawler() as crawler:
            # Run the crawler on the URL using the correct arun method
            result = await crawler.arun(url=url)
            # Return the markdown content
            return result.markdown if hasattr(result, 'markdown') else str(result)
    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return ''


def run_pipeline():
    db = SessionLocal()
    merger_agent = create_merger_agent()
    extractor_agent = create_deal_extractor_agent()
    crawler_agent = create_crawler_agent()

    articles = db.query(Article).filter_by(processed=False).all()
    articles=articles[:1]

    for article in articles:
        url = article.link

        # First, crawl the URL to get markdown content
        try:
            # Run the async crawling function
            markdown_content = asyncio.run(crawl_url(url))
        except Exception as e:
            logger.error(f"Error during crawling {url}: {e}")
            markdown_content = ""

        # Create a task for the crawler agent to process the markdown
        crawl_task = Task(
            description=f"Process this article content and clean it up into proper markdown format:\n\n{markdown_content}",
            expected_output="Clean, well-formatted markdown content of the article",
            agent=crawler_agent
        )

        # Only run the crawler agent if we have content
        if markdown_content:
            # Create a crew for the crawler task
            crawler_crew = Crew(
                agents=[crawler_agent],
                tasks=[crawl_task]
            )
            # Use the correct method to execute the crew
            crawl_result = crawler_crew.kickoff()
        else:
            # Fallback to the original approach if crawling failed
            try:
                raw_content = requests.get(url).text
                crawl_result = raw_content
            except Exception as e:
                logger.error(f"Error fetching content with requests: {e}")
                crawl_result = article.content or ""

        # Combine all available content
        full_text = f"{article.title}\n\n{article.summary}\n\n{crawl_result}"

        # Save the markdown content to the article
        # article.content = crawl_result
        # db.commit()

        classify_task = Task(
            description=f"Is this article about a deal or merger?\n\n{full_text}",
            expected_output="Return 'Yes' if it's about a deal/merger, otherwise 'No'.",
            agent=merger_agent
        )

        # Create a crew for the classification task
        classifier_crew = Crew(
            agents=[merger_agent],
            tasks=[classify_task]
        )
        # Use the correct method to execute the crew
        result = classifier_crew.kickoff()

        article.processed = True
        try:
            task_output = result.tasks_output[0].raw
            article.is_deal_related = "yes" in task_output.lower()
        except (IndexError, AttributeError) as e:
            logger.warning(f"Failed to parse classification output: {e}")
            article.is_deal_related = False
        db.commit()

        if article.is_deal_related:
            extract_task = Task(
                description=(
                    "Extract the following info: Buyer, Seller, Deal Amount, Date, and notes.\n\n"
                    f"Article:\n{full_text}"
                ),
                expected_output=(
                    "JSON with keys: buyer, seller, amount, date, additional_notes"
                ),
                agent=extractor_agent
            )

            # Create a crew for the extraction task
            extractor_crew = Crew(
                agents=[extractor_agent],
                tasks=[extract_task]
            )
            # Use the correct method to execute the crew
            extract_result = extractor_crew.kickoff()

            try:
                data = json.loads(extract_result)
                deal = Deal(
                    article_id=article.id,
                    buyer=data.get("buyer"),
                    seller=data.get("seller"),
                    amount=data.get("amount"),
                    date=data.get("date"),
                    additional_notes=data.get("additional_notes")
                )
                db.add(deal)
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to parse extraction result: {extract_result} - {e}")

    db.close()


if __name__ == '__main__':
    run_pipeline()