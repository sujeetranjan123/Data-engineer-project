from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    trim,
    lower,
    to_date,
    current_date,
    length,
    udf,
    lag,
    unix_timestamp,
    when,
    avg,
    stddev,
    sha2,
    substring,
    concat,
    sum as sql_sum,
    count,
    max as sql_max,
    min as sql_min,
    percentile_approx,
    collect_list,
    countDistinct,
    date_trunc,
    lit,
)
from pyspark.sql.types import BooleanType
from .config import S3Paths, MySQLConfig
from .secrets import get_secret


def create_spark_session(app_name: str = "medallion_etl") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # enable s3a; assume IAM role via instance profile
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )
    return spark


def read_mysql_table(spark: SparkSession, mysql_conf: MySQLConfig, secret_name: str) -> DataFrame:
    """Read a table from MySQL using credentials stored in AWS Secrets Manager."""
    secret = get_secret(secret_name)
    user = secret.get("username") or mysql_conf.user
    password = secret.get("password")

    df = (
        spark.read.format("jdbc")
        .option("url", mysql_conf.jdbc_url)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", f"{mysql_conf.database}.{mysql_conf.table}")
        .option("user", user)
        .option("password", password)
        .load()
    )
    return df

def write_bronze(df: DataFrame, paths: S3Paths) -> None:
    """Write raw DataFrame to bronze path, partitioned by ingestion date."""
    stamped = df.withColumn("ingestion_ts", current_timestamp())
    # partition by date for easier downstream compaction
    stamped = stamped.withColumn("ingestion_date", date_format(col("ingestion_ts"), "yyyy-MM-dd"))
    stamped.write.mode("append").partitionBy("ingestion_date").parquet(paths.bronze)


def transform_to_silver(df: DataFrame) -> DataFrame:
    """Perform cleansing operations to produce the silver layer.

    This function applies both general sanitization and a set of
    advanced, domain-specific checks useful for payments and
    banking datasets (velocity, BIN/MCC/IBAN checks, basic anomaly
    detection, IP/country consistency, PII hashing/masking).
    """
    clean = df

    # 1. drop rows with any null or NaN in critical columns
    critical_cols = [c for c, t in clean.dtypes if t not in ('string',)]
    if critical_cols:
        clean = clean.dropna(subset=critical_cols)
    clean = clean.na.drop()

    # 2. remove duplicate records (optionally specify a key)
    clean = clean.dropDuplicates()

    # 3. trim whitespace and 4. standardize casing for string columns
    string_cols = [c for c, t in clean.dtypes if t == 'string']
    for col_name in string_cols:
        clean = clean.withColumn(col_name, lower(trim(col(col_name))))

    # 5. email validation
    if 'email' in clean.columns:
        clean = clean.filter(col('email').rlike(r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'))

    # 6. basic numeric sanity
    if 'amount' in clean.columns:
        clean = clean.filter(col('amount') >= 0)

    # 7. parse and validate dates
    if 'event_date' in clean.columns:
        clean = clean.withColumn('event_date', to_date(col('event_date'), 'yyyy-MM-dd'))
        clean = clean.filter(col('event_date').isNotNull() & (col('event_date') <= current_date()))

    # 8. limit very long text
    for col_name in string_cols:
        clean = clean.filter(length(col(col_name)) <= 1000)

    # 9. drop non-ascii text
    for col_name in string_cols:
        clean = clean.filter(col(col_name).rlike(r'^[\\x00-\\x7F]*$'))

    # 10. ensure presence of key identifiers
    key_fields = ['id'] if 'id' in clean.columns else []
    for key in key_fields:
        clean = clean.filter(col(key) != "")

    # 11. payment-specific basic rules
    if 'amount' in clean.columns:
        clean = clean.filter(col('amount') > 0)
    if 'currency' in clean.columns:
        clean = clean.filter(col('currency').rlike(r'^[A-Z]{3}$'))
    valid_status = ['completed', 'pending', 'failed', 'refunded']
    if 'status' in clean.columns:
        clean = clean.filter(col('status').isin(valid_status))

    # 12. Luhn check for card numbers (UDF)
    def luhn_check(card_num: str) -> bool:
        if not card_num or not card_num.isdigit():
            return False
        digits = [int(d) for d in card_num]
        checksum = 0
        odd = False
        for d in reversed(digits):
            if odd:
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
            odd = not odd
        return checksum % 10 == 0

    luhn_udf = udf(luhn_check, BooleanType())
    if 'card_number' in clean.columns:
        clean = clean.filter(luhn_udf(col('card_number')))

    # 13. require merchant/acquirer ids
    for colname in ('merchant_id', 'acquirer_id'):
        if colname in clean.columns:
            clean = clean.filter(col(colname).isNotNull() & (col(colname) != ""))

    # 14. banking validations
    if 'iban' in clean.columns:
        clean = clean.filter(col('iban').rlike(r'^[A-Z]{2}[0-9A-Z]{13,32}$'))
    if 'transaction_date' in clean.columns and 'settlement_date' in clean.columns:
        clean = clean.withColumn('transaction_date', to_date(col('transaction_date')))
        clean = clean.withColumn('settlement_date', to_date(col('settlement_date')))
        clean = clean.filter(col('transaction_date').isNotNull() & col('settlement_date').isNotNull())
        clean = clean.filter(col('transaction_date') <= col('settlement_date'))
    banking_types = ['deposit', 'withdrawal', 'transfer', 'payment', 'fee']
    if 'transaction_type' in clean.columns:
        clean = clean.filter(col('transaction_type').isin(banking_types))
    if 'account_balance' in clean.columns:
        clean = clean.filter(col('account_balance') >= 0)
    if 'branch_code' in clean.columns:
        clean = clean.filter(col('branch_code').rlike(r'^[0-9]+$'))

    # 15. advanced/fraud-oriented filters (non-standard)
    # 15.a velocity: drop immediate repeat transactions on same card
    timestamp_col = None
    if 'event_timestamp' in clean.columns:
        timestamp_col = 'event_timestamp'
    elif 'ingestion_ts' in clean.columns:
        timestamp_col = 'ingestion_ts'

    if 'card_number' in clean.columns and timestamp_col:
        w = Window.partitionBy('card_number').orderBy(col(timestamp_col))
        clean = clean.withColumn('prev_ts', lag(col(timestamp_col)).over(w))
        # ts difference in seconds
        clean = clean.withColumn('ts_diff_secs', unix_timestamp(col(timestamp_col)) - unix_timestamp(col('prev_ts')))
        # drop transactions that occur within 30 seconds and are of suspicious size
        if 'amount' in clean.columns:
            clean = clean.filter(~((col('ts_diff_secs').isNotNull()) & (col('ts_diff_secs') < 30) & (col('amount') > 5000)))
        else:
            clean = clean.filter(~((col('ts_diff_secs').isNotNull()) & (col('ts_diff_secs') < 5)))

    # 15.b merchant anomalies: amount far outside merchant distribution
    if 'merchant_id' in clean.columns and 'amount' in clean.columns:
        stats = clean.groupBy('merchant_id').agg(avg('amount').alias('m_avg'), stddev('amount').alias('m_std'))
        clean = clean.join(stats, on='merchant_id', how='left')
        clean = clean.withColumn('zscore', (col('amount') - col('m_avg')) / (col('m_std') + 1e-9))
        clean = clean.filter((col('zscore').isNull()) | (abs(col('zscore')) <= 5))
        clean = clean.drop('m_avg', 'm_std', 'zscore')

    # 15.c MCC validation
    if 'mcc' in clean.columns:
        clean = clean.filter(col('mcc').rlike(r'^[0-9]{4}$'))
        suspicious_mcc = ['4829', '5967']  # example; replace with real list
        clean = clean.filter(~col('mcc').isin(suspicious_mcc))

    # 15.d IP / geo checks: require ip_country == card_country for high value txns
    if 'ip_country' in clean.columns and 'card_country' in clean.columns and 'amount' in clean.columns:
        clean = clean.filter(~((col('ip_country') != col('card_country')) & (col('amount') > 10000)))

    # 15.e mask / hash PII
    if 'card_number' in clean.columns:
        # keep last 4, first 6, mask middle
        clean = clean.withColumn('card_masked', concat(substring(col('card_number'), 1, 6), substring(col('card_number'), -4, 4)))
    if 'email' in clean.columns:
        clean = clean.withColumn('email_hash', sha2(col('email'), 256))
    if 'phone' in clean.columns:
        clean = clean.withColumn('phone_hash', sha2(col('phone'), 256))

    # 15.f cross-field consistency: currency vs country
    if 'currency' in clean.columns and 'card_country' in clean.columns:
        # example: drop if currency improbable for country (simple heuristic)
        eur_countries = ['fr', 'de', 'es', 'it']
        clean = clean.filter(~((col('currency') == 'EUR') & (~col('card_country').isin(eur_countries))))

    # drop temporary helper columns if present
    for tmp in ('prev_ts', 'ts_diff_secs'):
        if tmp in clean.columns:
            clean = clean.drop(tmp)

    return clean


def write_silver(df: DataFrame, paths: S3Paths) -> None:
    df.write.mode("overwrite").parquet(paths.silver)


def aggregate_to_gold(df: DataFrame) -> DataFrame:
    """Build gold dataset with business-level aggregates for payments/banking.

    This function creates analytical datasets suitable for dashboards, reporting,
    and business intelligence. It includes transaction metrics, merchant
    performance, customer segments, risk indicators, and temporal analysis.
    """
    gold = df

    # Ensure key columns exist for aggregation
    has_amount = 'amount' in gold.columns
    has_merchant = 'merchant_id' in gold.columns
    has_card = 'card_number' in gold.columns or 'card_masked' in gold.columns
    has_timestamp = 'event_timestamp' in gold.columns or 'transaction_date' in gold.columns
    has_status = 'status' in gold.columns
    has_country = 'card_country' in gold.columns

    # 1. Daily transaction summary (by merchant, country, type, etc.)
    if has_timestamp and has_merchant and has_amount:
        timestamp_col = 'event_timestamp' if 'event_timestamp' in gold.columns else 'transaction_date'
        daily_summary = (
            gold.withColumn('tx_date', date_trunc('day', col(timestamp_col)))
            .groupBy('tx_date', 'merchant_id')
            .agg(
                sql_sum('amount').alias('daily_tx_volume'),
                count('*').alias('daily_tx_count'),
                avg('amount').alias('daily_avg_amount'),
                sql_max('amount').alias('daily_max_amount'),
                sql_min('amount').alias('daily_min_amount'),
            )
            .select(
                col('tx_date'),
                col('merchant_id'),
                col('daily_tx_volume'),
                col('daily_tx_count'),
                col('daily_avg_amount'),
                col('daily_max_amount'),
                col('daily_min_amount'),
            )
        )
        # optionally cache or write this separately
        # daily_summary.write.mode('overwrite').parquet(paths.gold + '/daily_summary')

    # 2. Merchant performance metrics
    if has_merchant and has_amount and has_status:
        status_col = 'status' if has_status else None
        merchant_metrics = (
            gold.groupBy('merchant_id')
            .agg(
                sql_sum('amount').alias('merchant_total_volume'),
                count('*').alias('merchant_tx_count'),
                avg('amount').alias('merchant_avg_tx'),
                percentile_approx('amount', 0.5).alias('merchant_median_tx'),
                percentile_approx('amount', 0.95).alias('merchant_p95_tx'),
            )
        )
        if status_col:
            merchant_status = gold.groupBy('merchant_id', status_col).agg(
                count('*').alias(f'merchant_count_{status_col}')
            )
            merchant_metrics = merchant_metrics.join(merchant_status, on='merchant_id', how='left')

    # 3. Customer / cardholder metrics
    card_id_col = 'card_masked' if 'card_masked' in gold.columns else ('card_number' if has_card else None)
    if card_id_col and has_amount:
        customer_metrics = (
            gold.groupBy(card_id_col)
            .agg(
                sql_sum('amount').alias('customer_lifetime_value'),
                count('*').alias('customer_tx_count'),
                avg('amount').alias('customer_avg_tx'),
                sql_max('amount').alias('customer_max_single_tx'),
                countDistinct('merchant_id').alias('customer_unique_merchants') if has_merchant else lit(0),
            )
        )

    # 4. Risk / fraud indicators
    if has_amount and has_card:
        risk_metrics = (
            gold.filter(col('status') == 'failed')
            .groupBy(card_id_col)
            .agg(
                count('*').alias('failed_tx_count'),
                (count('*') / countDistinct('merchant_id')).alias('failed_tx_per_merchant') if has_merchant else lit(0),
            )
        )

    # 5. Geographic distribution
    if has_country and has_amount:
        geo_metrics = (
            gold.groupBy('card_country')
            .agg(
                sql_sum('amount').alias('country_volume'),
                count('*').alias('country_tx_count'),
                countDistinct('merchant_id').alias('country_unique_merchants') if has_merchant else lit(0),
                countDistinct(card_id_col).alias('country_unique_customers') if card_id_col else lit(0),
            )
        )

    # 6. Top merchants, cards, countries
    if has_merchant and has_amount:
        top_merchants = (
            gold.groupBy('merchant_id')
            .agg(sql_sum('amount').alias('merchant_volume'), count('*').alias('merchant_count'))
            .orderBy(col('merchant_volume').desc())
            .limit(100)
        )

    if card_id_col and has_amount:
        top_customers = (
            gold.groupBy(card_id_col)
            .agg(sql_sum('amount').alias('customer_volume'), count('*').alias('customer_count'))
            .orderBy(col('customer_volume').desc())
            .limit(100)
        )

    if has_country and has_amount:
        top_countries = (
            gold.groupBy('card_country')
            .agg(sql_sum('amount').alias('country_volume'), count('*').alias('country_count'))
            .orderBy(col('country_volume').desc())
            .limit(50)
        )

    # 7. Hourly transaction trends
    if has_timestamp and has_amount:
        timestamp_col = 'event_timestamp' if 'event_timestamp' in gold.columns else 'transaction_date'
        hourly_trends = (
            gold.withColumn('tx_hour', date_trunc('hour', col(timestamp_col)))
            .groupBy('tx_hour')
            .agg(
                sql_sum('amount').alias('hourly_volume'),
                count('*').alias('hourly_tx_count'),
                avg('amount').alias('hourly_avg_amount'),
            )
            .orderBy('tx_hour')
        )

    # 8. Transaction status breakdown
    if has_status and has_amount:
        status_summary = (
            gold.groupBy('status')
            .agg(
                sql_sum('amount').alias('status_volume'),
                count('*').alias('status_count'),
                avg('amount').alias('status_avg_amount'),
            )
        )
        # Calculate success rate
        total_count = gold.count()
        success_rate = (
            gold.filter(col('status') == 'completed').count() / total_count * 100
            if total_count > 0
            else 0
        )

    # 9. Create a unified gold table combining key metrics
    # This is the primary output for downstream analytics
    gold_output = (
        gold
        .withColumn('ingestion_date', current_date())
        .withColumn('processing_timestamp', current_timestamp())
    )

    # Optional: enrich with computed fields for dashboards
    if has_amount and has_status:
        gold_output = gold_output.withColumn(
            'is_success',
            when(col('status') == 'completed', 1).otherwise(0)
        )

    if card_id_col and has_timestamp:
        timestamp_col = 'event_timestamp' if 'event_timestamp' in gold.columns else 'transaction_date'
        gold_output = gold_output.withColumn(
            'tx_date',
            date_trunc('day', col(timestamp_col))
        )

    # Drop or keep PII columns based on compliance requirements
    # For now, we keep them if present (adjust as needed)

    return gold_output


def write_gold(df: DataFrame, paths: S3Paths) -> None:
    df.write.mode("overwrite").parquet(paths.gold)
