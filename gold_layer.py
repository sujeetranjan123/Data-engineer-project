"""Standalone script to run gold layer aggregations on EMR."""
from medallion.config import S3Paths
from medallion.etl import (
    create_spark_session,
    aggregate_to_gold,
    write_gold,
)


def run():
    paths = S3Paths(
        bronze="s3a://my-bucket/bronze/",
        silver="s3a://my-bucket/silver/",
        gold="s3a://my-bucket/gold/",
    )

    spark = create_spark_session(app_name="gold_layer")

    silver_df = spark.read.parquet(paths.silver)
    gold_df = aggregate_to_gold(silver_df)
    write_gold(gold_df, paths)


if __name__ == "__main__":
    run()
