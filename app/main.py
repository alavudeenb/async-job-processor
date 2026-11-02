from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app import models
from app.routers import jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Async Job Processing API", lifespan=lifespan)

app.include_router(jobs.router)


@app.get("/")
def Welcome():
    return {"Response": "Welcome"}


@app.get("/health")
def health():
    return {"status": "health"}
