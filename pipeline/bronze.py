from pyspark.sql import SparkSession
from src.config import olist_config
from src.ingestion import ingest_olist_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    spark = None

    try:
        # Create local Spark session
        spark = (
            SparkSession.builder
            .appName("OlistBronze")
            .master("local[*]")
            .getOrCreate()
        )

        # Read each raw Olist CSV
        for name, config in olist_config.items():

            try:
                df = ingest_olist_data(
                    spark=spark,
                    raw_path_to_file=config["filename"],
                    file_schema=config["schema"]
                )

                # Write dataset to Bronze layer as Parquet
                bronze_path = f"data/bronze/{name}"

                df.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .save(bronze_path)

                logger.info(
                    f"Successfully wrote {name} to {bronze_path}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to ingest {config['filename']}: {e}"
                )
                raise

        logger.info("Bronze ingestion completed successfully")

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()