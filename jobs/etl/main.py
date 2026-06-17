from pathlib import Path

import pandas as pd

from ..config import RAW_DATA_DIR
from ..utils.logger import get_logger
from .extraction import (
    WeatherExtractor,
    get_extraction_window,
    load_default_locations,
    parse_to_dataframe,
)
from .load import WeatherDataLoader
from .transform import WeatherDataTransformer

logger = get_logger(__name__)


class WeatherEtlPipeline:
    """Orchestrates the full Extract → Transform → Load pipeline."""

    def __init__(self):
        self._extractor = WeatherExtractor()
        self._transformer = WeatherDataTransformer()
        self._loader = WeatherDataLoader()

    def run(self) -> None:
        logger.info("ETL pipeline starting")
        try:
            raw_df = self._extract()
            self._save_raw(raw_df)
            transformed_df = self._transformer.transform(raw_df)
            self._loader.initialize_schema()
            self._loader.load(transformed_df)
            logger.info("ETL pipeline finished successfully")
        except Exception as error:
            logger.exception("ETL pipeline failed: %s", error)
            raise

    def _extract(self) -> pd.DataFrame:
        locations = load_default_locations()
        start_date, end_date = get_extraction_window()

        with self._extractor as extractor:
            results = extractor.extract_all(locations, start_date, end_date)

        frames = [parse_to_dataframe(result) for result in results]
        combined = pd.concat(frames, ignore_index=True)
        logger.info("Extract phase done | rows=%d", len(combined))
        return combined

    def _save_raw(self, raw_df: pd.DataFrame) -> None:
        raw_dir = Path(RAW_DATA_DIR)
        raw_dir.mkdir(parents=True, exist_ok=True)

        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        file_path = raw_dir / f"weather_raw_{timestamp}.csv"
        raw_df.to_csv(file_path, index=False)

        logger.info("Raw data saved | path=%s | shape=%s", file_path, raw_df.shape)
        logger.info(
            "--- RAW DATA PREVIEW (first 5 rows) ---\n%s",
            raw_df.head().to_string(index=False),
        )
        logger.info(
            "--- COLUMN TYPES ---\n%s",
            raw_df.dtypes.to_string(),
        )
        logger.info(
            "--- MISSING VALUES ---\n%s",
            raw_df.isnull().sum().to_string(),
        )


def main() -> None:
    pipeline = WeatherEtlPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
