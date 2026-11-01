import os
from dotenv import load_dotenv

load_dotenv()

REGISTERED_JOB_TYPES = {
    item.strip()
    for item in os.getenv("REGISTERED_JOB_TYPES", "").split(",")
    if item.strip()
}

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
