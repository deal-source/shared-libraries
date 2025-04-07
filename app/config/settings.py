from typing import ClassVar, List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_URL: str = "postgresql+psycopg2://postgres:satya@localhost:5432/rss_monitor"
    FEEDS: ClassVar[List[str]] = [
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRXEMGSQ5SSFVUF0xXEkZcXEUDGkRJXhlfVlJXGQ==",
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJdEVhZXw==",
        "https://techcrunch.com/feed/"
    ]

settings = Settings()