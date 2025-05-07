# agents.py
from crewai import Agent
from crewai.tools import BaseTool
from textwrap import dedent
import requests
import random
import time
import logging
from typing import Optional, Any
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Set up logging for the API calls
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
api_logger = logging.getLogger("forager_api")


# Create a custom tool for company website lookup
class CompanyWebsiteTool(BaseTool):
    name: str = "fetch_company_website"
    description: str = "Looks up the official website for a company using the Forager API"

    def _run(self, company_name: str) -> str:
        """
        Uses the Forager API to fetch the official website for a company.

        Args:
            company_name: The name of the company to look up

        Returns:
            The company's website domain or "Not found" if unavailable
        """
        if not company_name or company_name.strip() == "":
            return "No company name provided"

        try:
            # Format query and remove any special characters that might interfere
            query = company_name.strip()

            # Add random delay to avoid rate limiting (0.5-2 seconds)
            time.sleep(random.uniform(0.5, 2))

            # Make API request
            url = f"https://api-v2.forager.ai/api/datastorage/autocomplete/organizations/?q={query}"
            api_logger.info(f"Looking up website for: {query}")

            response = requests.get(url, timeout=10)

            # Handle rate limiting with retry
            if response.status_code == 429:
                api_logger.warning(f"Rate limited when looking up {query}, retrying after delay")
                time.sleep(random.uniform(5, 10))  # Longer delay for retry
                response = requests.get(url, timeout=10)

            if response.status_code != 200:
                api_logger.error(f"API error ({response.status_code}) for {query}: {response.text}")
                return f"API error: {response.status_code}"

            data = response.json()

            # Check if we have results
            if data and "results" in data and data["results"]:
                # Extract website from the text field (format: "Company Name - website.com")
                result = data["results"][0]
                text_parts = result["text"].split(" - ")

                if len(text_parts) > 1:
                    # Return just the website portion
                    return text_parts[1]
                else:
                    return "Website format not recognized"
            else:
                api_logger.warning(f"No results found for {query}")
                return "Not found"

        except Exception as e:
            api_logger.error(f"Error looking up website for {company_name}: {str(e)}")
            return f"Error: {str(e)}"


def create_merger_agent(model="gpt-4.1-mini"):
    return Agent(
        role="Deal Identification Specialist",
        goal="Accurately identify financial transactions between companies or investors",
        backstory=dedent("""
            Financial news analyst specializing in identifying various transaction types:
            - Mergers and acquisitions (M&A)
            - Private equity investments and buyouts
            - Venture capital funding rounds
            - Debt financing and bond issuances
            - IPOs and follow-on offerings
            - Minority stake acquisitions
            - Joint ventures and partnerships
            - Asset sales and divestitures
            With ability to quickly distinguish deal-related content from general business news.
        """),
        verbose=True,
        llm=model
    )


def create_deal_extractor_agent(model="gpt-4.1-mini"):
    return Agent(
        role="Financial Data Extraction Specialist",
        goal="Extract comprehensive deal information in precise JSON format",
        backstory=dedent("""
            Expert financial analyst skilled in extracting transaction details from news articles,
            press releases, and regulatory filings. Specialized in identifying:

            1. Transaction participants (buyers, sellers, investors, companies)
            2. Deal values, pricing, and financial terms
            3. Transaction dates and timelines
            4. Geographic scope and jurisdictions
            5. Deal rationale and strategic implications

            Always formats output as clean, parsed JSON with consistent field names to ensure
            seamless data pipeline integration.
        """),
        verbose=True,
        llm=model
    )


def create_website_lookup_agent(model="gpt-4.1-mini"):
    # Create an instance of our custom tool
    company_website_tool = CompanyWebsiteTool()

    return Agent(
        role="Company Website Researcher",
        goal="Find official websites for companies involved in financial transactions",
        backstory=dedent("""
            Data enrichment specialist focused on corporate entity validation and identification.
            Expert at using external APIs and databases to locate and verify company websites
            and digital footprints. Ensures all entities in financial transactions have complete
            and accurate metadata, particularly their official web domains.
        """),
        tools=[company_website_tool],
        verbose=True,
        llm=model
    )


def create_crawler_agent(model="gpt-4.1-mini"):
    return Agent(
        role="Financial Content Crawler",
        goal="Extract complete article content while preserving critical financial data",
        backstory=dedent("""
            Web scraping specialist with expertise in financial news sites, press releases,
            and regulatory filings. Capable of extracting clean, complete content while 
            preserving tables, numbers, dates, and organizational structures essential for 
            financial analysis. Implements rate limiting and respects robots.txt to avoid
            being blocked by websites.
        """),
        verbose=True,
        llm=model
    )


class DatabaseWriterTool(BaseTool):
    name: str = "write_company_to_database"
    description: str = "Writes company information to the PostgreSQL database"

    def _run(self, company_name: str, company_website: str = "") -> str:
        """
        Writes company information to the PostgreSQL database.

        Args:
            company_name: The name of the company
            company_website: The website of the company (optional)

        Returns:
            A message indicating success or failure
        """
        if not company_name or company_name.strip() == "":
            return "No company name provided"

        try:
            # Connection parameters - these should be configured in environment variables
            # or a config file in a production environment
            conn_params = {
                "dbname": "dealsource_raw",
                "user": "postgres",
                "password": "SatyaSem#admin1",  # Should be from environment variable
                "host": "34.91.220.83",
                "port": "5432"
            }


            # Connect to the database
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    # Check if the company already exists
                    cur.execute(
                        "SELECT company_name FROM source_db.company_list_forager_input WHERE company_name = %s",
                        (company_name,)
                    )

                    # If company doesn't exist, insert it
                    if cur.fetchone() is None:
                        cur.execute(
                            "INSERT INTO source_db.company_list_forager_input (company_name, company_website) VALUES (%s, %s)",
                            (company_name, company_website)
                        )
                        return f"Added new company: {company_name} with website: {company_website}"
                    else:
                        # Update the website if it's provided and different from what's stored
                        if company_website:
                            cur.execute(
                                "UPDATE source_db.company_list_forager_input SET company_website = %s WHERE company_name = %s AND (company_website IS NULL OR company_website != %s)",
                                (company_website, company_name, company_website)
                            )
                            if cur.rowcount > 0:
                                return f"Updated website for company: {company_name} to {company_website}"
                            else:
                                return f"Company {company_name} already exists with the same website"
                        else:
                            return f"Company {company_name} already exists"

        except Exception as e:
            # logger.error(f"❌ Database error for {company_name}: {str(e)}")
            print(f"❌ Database error for {company_name}: {str(e)}")
            return f"Error: {str(e)}"


def create_database_writer_agent(model="gpt-4.1-mini"):
    # Create an instance of our custom tool
    db_writer_tool = DatabaseWriterTool()

    return Agent(
        role="Database Writer",
        goal="Store company information in the PostgreSQL database",
        backstory=dedent("""
            Database specialist responsible for maintaining accurate company records.
            Ensures all companies involved in financial transactions are properly
            recorded in the database for future reference and analysis.
            Handles duplicate detection and data validation to maintain database integrity.
        """),
        tools=[db_writer_tool],
        verbose=True,
        llm=model
    )