from contextlib import asynccontextmanager

from fastapi import FastAPI, status, Request
from fastapi.responses import JSONResponse

from app.storagelayer.database import Base, engine
from app.storagelayer import models
from app.api.routers import routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Async Job Processing API", lifespan=lifespan)

MAX_REQUEST_BODY_BYTES = 100_00  # ~100 KB total HTTP body limit


@app.middleware("http")
async def limit_request_body_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_REQUEST_BODY_BYTES:
        return JSONResponse(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content={"detail": "Request body too large"},
        )
    return await call_next(request)


app.include_router(routes.apirouter)
