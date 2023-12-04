from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def read_categories(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/source/{date}/categories")


def read_stations(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return spark.read.json(f"{bucket_path}/source/{date}/stations")


def read_timeseries(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/source/{date}/timeseries")


def read_timeseriesdata(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/source/{date}/timeseriesdata")


def __read_from_s3(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.json(path)
