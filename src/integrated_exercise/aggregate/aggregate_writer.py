from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

import src.integrated_exercise.aggregate.snowflake_credentials_manager as snowflake_credentials_manager


def write_dataframe(dataframe: DataFrame, spark: SparkSession, bucket_path: str, date: str, key: str):
    __write_to_s3(dataframe, bucket_path, date, key)
    __write_to_snowflake(dataframe, spark, date, key)


def __write_to_s3(dataframe: DataFrame, bucket_path: str, date: str, key: str):
    lower_key = str.lower(key)
    dataframe.write.parquet(f"{bucket_path}/derived/{date}/{lower_key}/", mode="overwrite")


def __write_to_snowflake(dataframe: DataFrame, spark: SparkSession, date: str, key: str):
    upper_key = str.upper(key)
    snowflake_options = snowflake_credentials_manager.get_snowflake_creds_from_secret_manager()
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(snowflake_options, f"DELETE FROM {upper_key} WHERE DS='{date}'")
    dataframe.write.format("net.snowflake.spark.snowflake").options(**snowflake_options).option("dbtable", f"ACADEMY_DBT.AXXES_ALEX.{upper_key}").mode('append').options(header=True).save()