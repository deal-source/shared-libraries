from crewai import Agent

def create_merger_agent():
    return Agent(
        role="Merger Classifier",
        goal="Identify if article relates to mergers or acquisitions",
        backstory="Financial news expert.",
        verbose=True
    )

def create_deal_extractor_agent():
    return Agent(
        role="Deal Extractor",
        goal="Extract deal data from articles",
        backstory="Expert at pulling structured info from unstructured financial news",
        verbose=True
    )

def create_crawler_agent():
    return Agent(
        role="Web Crawler",
        goal="Extract article content and convert to markdown format",
        backstory="Expert web scraper that can extract clean, readable content from financial news websites",
        verbose=True
    )


