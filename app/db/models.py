from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base
from datetime import datetime

class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    link = Column(Text, unique=True)
    summary = Column(Text)
    content = Column(Text)
    published = Column(DateTime, default=datetime.utcnow)
    source = Column(Text)
    processed = Column(Boolean, default=False)
    is_deal_related = Column(Boolean)

    deal = relationship("Deal", back_populates="article", uselist=False)

class Deal(Base):
    __tablename__ = "deals"
    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey("articles.id"))
    buyer = Column(Text)
    seller = Column(Text)
    amount = Column(String)
    date = Column(String)
    additional_notes = Column(Text)

    article = relationship("Article", back_populates="deal")