from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import (
    API_BASE_URL,
    DEFAULT_LOCATIONS,
    EXTRACTION_LOOKBACK_DAYS,
    HOURLY_VARIABLES,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_ON_STATUS_CODES,
)
from ..utils.logger import get_logger

logger = get_logger(__name__)

REQUIRED_RESPONSE_KEYS = {"latitude", "longitude", "hourly", "hourly_units"}


@dataclass(frozen=True)
class Location:
    name: str
    country: str
    latitude: float
    longitude: float
    timezone: str = "auto"


@dataclass(frozen=True)
class ExtractionResult:
    location: Location
    raw_data: dict
    extracted_at: str
    records_count: int


class WeatherExtractor:
    """Extracts hourly weather data from the Open-Meteo API.

    Handles transport-level retries (exponential back-off via HTTPAdapter),
    response-schema validation, and structured logging so every run is
    fully auditable.
    """

    def __init__(self, base_url: str = API_BASE_URL):
        self._base_url = base_url
        self._session = self._build_session()

    # ------------------------------------------------------------------ #
    # Public interface                                                      #
    # ------------------------------------------------------------------ #

    def extract_all(
        self,
        locations: list[Location],
        start_date: date,
        end_date: date,
    ) -> list[ExtractionResult]:
        results = [self.extract(loc, start_date, end_date) for loc in locations]
        total_records = sum(r.records_count for r in results)
        logger.info(
            "Batch extraction complete | locations=%d | total_records=%d",
            len(results),
            total_records,
        )
        return results

    def extract(
        self,
        location: Location,
        start_date: date,
        end_date: date,
    ) -> ExtractionResult:
        logger.info(
            "Starting extraction | location=%s, %s | range=%s → %s",
            location.name,
            location.country,
            start_date,
            end_date,
        )
        params = self._build_params(location, start_date, end_date)
        raw = self._fetch(params)
        self._validate_response_schema(raw)

        records_count = len(raw["hourly"]["time"])
        logger.info(
            "Extraction succeeded | location=%s | records=%d",
            location.name,
            records_count,
        )
        return ExtractionResult(
            location=location,
            raw_data=raw,
            extracted_at=pd.Timestamp.now(tz="UTC").isoformat(),
            records_count=records_count,
        )

    # ------------------------------------------------------------------ #
    # Context-manager support                                              #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "WeatherExtractor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._session.close()
        return False

    # ------------------------------------------------------------------ #
    # Private helpers                                                       #
    # ------------------------------------------------------------------ #

    def _fetch(self, params: dict) -> dict:
        try:
            response = self._session.get(
                self._base_url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as error:
            logger.error("Connection error while calling the API: %s", error)
            raise
        except requests.exceptions.Timeout as error:
            logger.error(
                "API request timed out after %ds: %s",
                REQUEST_TIMEOUT_SECONDS,
                error,
            )
            raise
        except requests.exceptions.HTTPError as error:
            logger.error(
                "HTTP %d from API: %s",
                response.status_code,
                error,
            )
            raise
        except requests.exceptions.JSONDecodeError as error:
            logger.error("API returned a non-JSON body: %s", error)
            raise

    def _build_session(self) -> requests.Session:
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=list(RETRY_ON_STATUS_CODES),
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _build_params(
        self,
        location: Location,
        start_date: date,
        end_date: date,
    ) -> dict:
        return {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(HOURLY_VARIABLES),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": location.timezone,
        }

    def _validate_response_schema(self, response: dict) -> None:
        missing_top_level = REQUIRED_RESPONSE_KEYS - response.keys()
        if missing_top_level:
            raise ValueError(
                f"API response missing required top-level fields: {missing_top_level}"
            )

        if "time" not in response["hourly"]:
            raise ValueError("API response 'hourly' block is missing the 'time' field")

        missing_variables = [
            var for var in HOURLY_VARIABLES if var not in response["hourly"]
        ]
        if missing_variables:
            logger.warning(
                "API response missing expected hourly variables: %s",
                missing_variables,
            )


# ------------------------------------------------------------------ #
# Module-level helpers                                                 #
# ------------------------------------------------------------------ #

def build_location(config: dict) -> Location:
    return Location(
        name=config["name"],
        country=config["country"],
        latitude=config["latitude"],
        longitude=config["longitude"],
        timezone=config.get("timezone", "auto"),
    )


def load_default_locations() -> list[Location]:
    return [build_location(cfg) for cfg in DEFAULT_LOCATIONS]


def get_extraction_window(
    lookback_days: int = EXTRACTION_LOOKBACK_DAYS,
) -> tuple[date, date]:
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date


def parse_to_dataframe(result: ExtractionResult) -> pd.DataFrame:
    """Flattens an ExtractionResult into a DataFrame ready for the transform step."""
    df = pd.DataFrame(result.raw_data["hourly"])
    df["location_name"] = result.location.name
    df["location_country"] = result.location.country
    df["latitude"] = result.location.latitude
    df["longitude"] = result.location.longitude
    df["extracted_at"] = result.extracted_at
    return df
