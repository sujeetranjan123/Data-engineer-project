from medallion.config import S3Paths, MySQLConfig
from medallion.etl import (
    create_spark_session,
    read_mysql_table,
    write_bronze,
    transform_to_silver,
    write_silver,
    aggregate_to_gold,
    write_gold,
)


def main():
    # configure paths and DB settings
    paths = S3Paths(
        bronze="s3a://my-bucket/bronze/",
        silver="s3a://my-bucket/silver/",
        gold="s3a://my-bucket/gold/",
    )

    mysql_conf = MySQLConfig(
        host="my-mysql-host",
        port=3306,
        database="mydatabase",
        table="mytable",
        user=None,  # will be supplied by secret
        jdbc_url="jdbc:mysql://my-mysql-host:3306",
    )

    spark = create_spark_session()

    # read raw data into bronze layer
    raw_df = read_mysql_table(spark, mysql_conf, secret_name="prod/mysql/credentials")
    write_bronze(raw_df, paths)

    # transform into silver layer
    silver_df = transform_to_silver(raw_df)
    write_silver(silver_df, paths)

    # aggregate into gold layer
    gold_df = aggregate_to_gold(silver_df)
    write_gold(gold_df, paths)


if __name__ == "__main__":
    main()
