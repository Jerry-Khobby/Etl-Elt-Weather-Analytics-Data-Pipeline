import pandas as pd

from ..etl.extraction import (
    WeatherExtractor,
    get_extraction_window,
    load_default_locations,
    parse_to_dataframe,
)
from ..utils.logger import get_logger

logger = get_logger(__name__)


def extract_raw() -> pd.DataFrame:
    """Extracts raw weather data for all default locations.

    Returns a DataFrame with unmodified API columns ready for staging.
    Reuses the ETL WeatherExtractor — no transformation is applied here.
    """
    locations = load_default_locations()
    start_date, end_date = get_extraction_window()

    with WeatherExtractor() as extractor:
        results = extractor.extract_all(locations, start_date, end_date)

    frames = [parse_to_dataframe(result) for result in results]
    combined = pd.concat(frames, ignore_index=True)
    logger.info(
        "ELT raw extraction complete | rows=%d | locations=%d",
        len(combined),
        len(locations),
    )
    return combined
