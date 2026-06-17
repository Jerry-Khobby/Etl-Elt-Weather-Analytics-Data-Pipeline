import os

from dotenv import load_dotenv

load_dotenv()

# --- API ---
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "surface_pressure",
    "visibility",
    "uv_index",
    "is_day",
]

# --- Retry ---
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2          # wait 2s, 4s, 8s between attempts
REQUEST_TIMEOUT_SECONDS = 30
RETRY_ON_STATUS_CODES = (429, 500, 502, 503, 504)

# --- Extraction window ---
EXTRACTION_LOOKBACK_DAYS = 7

# --- Locations ---
DEFAULT_LOCATIONS = [
    {
        "name": "Accra",
        "country": "Ghana",
        "latitude": 5.5560,
        "longitude": -0.1969,
        "timezone": "Africa/Accra",
    },
    {
        "name": "London",
        "country": "United Kingdom",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "timezone": "Europe/London",
    },
    {
        "name": "New York",
        "country": "United States",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "timezone": "America/New_York",
    },
    {
        "name": "Tokyo",
        "country": "Japan",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "timezone": "Asia/Tokyo",
    },
    {
        "name": "Berlin",
        "country": "Germany",
        "latitude": 52.5200,
        "longitude": 13.4050,
        "timezone": "Europe/Berlin",
    },
]

# --- Analytics Database (PostgreSQL star schema) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "weather_analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# --- Staging Database (PostgreSQL, ELT raw loads) ---
STAGING_DB_HOST = os.getenv("STAGING_DB_HOST", "localhost")
STAGING_DB_PORT = int(os.getenv("STAGING_DB_PORT", "5434"))
STAGING_DB_NAME = os.getenv("STAGING_DB_NAME", "staging_db")
STAGING_DB_USER = os.getenv("STAGING_DB_USER", "staging_user")
STAGING_DB_PASSWORD = os.getenv("STAGING_DB_PASSWORD", "")

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "logs")

# --- Raw data output ---
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
