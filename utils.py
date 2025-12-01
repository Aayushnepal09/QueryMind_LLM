import os
from dotenv import load_dotenv
from urllib.parse import quote_plus



load_dotenv()  # reads variables from a .env file and sets them in os.environ


def get_db_url():
    POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")   # can be host or host:port
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")

    if not all([POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_SERVER, POSTGRES_DATABASE]):
        raise ValueError("Missing DB env vars. Check .env for POSTGRES_USERNAME/PASSWORD/SERVER/DATABASE")

    # URL-encode password (very important)
    pw = quote_plus(POSTGRES_PASSWORD)

    # Add sslmode=require for Render
    return f"postgresql://{POSTGRES_USERNAME}:{pw}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}?sslmode=require"
