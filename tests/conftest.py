import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

from app.api import main
from app.api.main import app
from app.storagelayer import database
from app.storagelayer.database import Base, get_db
from app.api.businesslayer import jobprocessor
from app.worker import worker


@pytest.fixture
def client(monkeypatch):  # 1. Add monkeypatch parameter
    test_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=test_engine,
    )

    # 2. Point both jobprocessor and database to your in-memory SQLite test DB
    monkeypatch.setattr(main, "engine", test_engine)
    monkeypatch.setattr(jobprocessor, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(database, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(worker, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(database, "engine", test_engine)
    monkeypatch.setattr(main, "engine", test_engine)

    Base.metadata.create_all(bind=test_engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        test_client.db_session = TestingSessionLocal
        yield test_client

    app.dependency_overrides.clear()
    Base.metadata.drop_all(bind=test_engine)
    test_engine.dispose()
