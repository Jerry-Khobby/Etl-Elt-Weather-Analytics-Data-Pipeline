from ..etl.load import WeatherDataLoader
from ..utils.logger import get_logger
from .extract import extract_raw
from .load import StagingLoader
from .transform import StagingTransformer

logger = get_logger(__name__)


class WeatherEltPipeline:
    """Runs the full ELT flow.

    1. Extract  — pull raw data from the Open-Meteo API.
    2. Load     — insert raw rows into the PostgreSQL staging table as-is.
    3. Transform — execute a SQL SELECT on the staging table that casts types,
                   renames columns, and computes derived fields inside the DB.
    4. Load     — insert the transformed rows into the analytics star schema.
    5. Mark     — flag processed staging rows so they are not reprocessed.
    """

    def __init__(self):
        self._staging_loader = StagingLoader()
        self._transformer = StagingTransformer()
        self._analytics_loader = WeatherDataLoader()

    def run(self) -> None:
        logger.info("ELT pipeline starting")
        try:
            raw_df = extract_raw()
            self._staging_loader.initialize_schema()
            self._staging_loader.load(raw_df)

            transformed_df, staging_ids = self._transformer.transform()
            if transformed_df.empty:
                logger.info("No new staging data to process — ELT pipeline complete")
                return

            self._analytics_loader.initialize_schema()
            self._analytics_loader.load(transformed_df)
            self._staging_loader.mark_processed(staging_ids)
            logger.info("ELT pipeline finished successfully")
        except Exception as error:
            logger.exception("ELT pipeline failed: %s", error)
            raise


def main() -> None:
    pipeline = WeatherEltPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
