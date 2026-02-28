"""Standalone script to run silver layer transformations on EMR."""
from medallion.config import S3Paths, MySQLConfig
from medallion.etl import (
    create_spark_session,
    # read_mysql_table,  # silver layer might read from bronze instead
    transform_to_silver,
    write_silver,
)


def run():
    paths = S3Paths(
        bronze="s3a://my-bucket/bronze/",
        silver="s3a://my-bucket/silver/",
        gold=None,
    )

    spark = create_spark_session(app_name="silver_layer")

    # read bronze data
    bronze_df = spark.read.parquet(paths.bronze)
    silver_df = transform_to_silver(bronze_df)
    write_silver(silver_df, paths)


if __name__ == "__main__":
    run()
