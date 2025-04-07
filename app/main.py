from fastapi import FastAPI
from app.api.v1 import rss

app = FastAPI()
app.include_router(rss.router, prefix="/api/v1")