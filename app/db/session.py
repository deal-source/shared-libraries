from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config.settings import settings

engine = create_engine(settings.DB_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)