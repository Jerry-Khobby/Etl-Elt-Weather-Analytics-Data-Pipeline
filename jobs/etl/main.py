import sys

import pandas as pd

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


def main() -> None:
    pipeline = WeatherEtlPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
