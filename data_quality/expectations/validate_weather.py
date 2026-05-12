import os
import logging
import pandas as pd
import great_expectations as gx
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

DB_USER     = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST     = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT     = os.getenv("POSTGRES_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB", "airflow")


def load_raw_data() -> pd.DataFrame:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    df = pd.read_sql("SELECT * FROM raw_weather", engine)
    logger.info(f"Loaded {len(df)} records from raw_weather table")
    return df


def run_validation(df: pd.DataFrame) -> bool:
    logger.info("=" * 50)
    logger.info("Starting data quality validation")
    logger.info("=" * 50)

    context     = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("weather_datasource")
    data_asset  = data_source.add_dataframe_asset("raw_weather_asset")
    batch_def   = data_asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.add(
        gx.ExpectationSuite(name="raw_weather_suite")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="city")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="country")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="temperature",
            min_value=-90,
            max_value=60
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="humidity",
            min_value=0,
            max_value=100
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="wind_speed",
            min_value=0,
            max_value=200
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1,
            max_value=None
        )
    )

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="weather_validation",
            data=batch_def,
            suite=suite
        )
    )

    results    = validation_def.run(batch_parameters={"dataframe": df})
    all_passed = True

    for result in results.results:
        expectation_type = result.expectation_config.type
        status           = "PASS" if result.success else "FAIL"
        logger.info(f"{status} | {expectation_type}")
        if not result.success:
            all_passed = False
            logger.warning(f"  Failed: {result.result}")

    logger.info("=" * 50)
    if all_passed:
        logger.info("All expectations passed — data is clean")
    else:
        logger.error("Some expectations failed — check data quality")
    logger.info("=" * 50)

    return all_passed


def main():
    df     = load_raw_data()
    passed = run_validation(df)
    if not passed:
        raise ValueError("Data quality validation failed")


if __name__ == "__main__":
    main()
