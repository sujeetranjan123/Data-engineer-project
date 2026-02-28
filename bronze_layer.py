"""Standalone script to run the bronze layer on EMR."""
from medallion.config import S3Paths, MySQLConfig
from medallion.etl import (
    create_spark_session,
    read_mysql_table,
    write_bronze,
)


def run():
    paths = S3Paths(
        bronze="s3a://my-bucket/bronze/",
        silver=None,
        gold=None,
    )

    mysql_conf = MySQLConfig(
        host="my-mysql-host",
        port=3306,
        database="mydatabase",
        table="mytable",
        user=None,
        jdbc_url="jdbc:mysql://my-mysql-host:3306",
    )

    spark = create_spark_session(app_name="bronze_layer")

    raw_df = read_mysql_table(spark, mysql_conf, secret_name="prod/mysql/credentials")
    write_bronze(raw_df, paths)


if __name__ == "__main__":
    run()
