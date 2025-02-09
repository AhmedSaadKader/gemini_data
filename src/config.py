import os
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_NAME = "drug_database"
DB_USER = "postgres"
DB_HOST = "localhost"

# Load database password and handle potential missing environment variable
DB_PASSWORD = os.getenv("DB_PASSWORD")
if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD not found in .env file")

# Gemini API Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in .env file")


# Add a check to ensure variables are not None/empty strings
if not all([DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, GOOGLE_API_KEY]):
    raise ValueError("One or more configuration variables are missing or empty.")