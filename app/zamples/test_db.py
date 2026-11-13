from sqlalchemy import text
from app.storagelayer.database import engine

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print("Database connected:", result.scalar())
