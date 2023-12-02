from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    MapType,
    ArrayType
)

station_fields = [
    StructField("properties", StructType(fields=[
        StructField("id", IntegerType()),
        StructField("label", StringType()),
        StructField("timeseries", MapType(StringType(), MapType(StringType(), StructType(fields=[
            StructField("id", StringType()), StructField("label", StringType())
        ]))))
    ])),
    StructField("geometry", StructType(fields=[
        StructField("coordinates", ArrayType(FloatType())),
        StructField("type", StringType())
    ])),
    StructField("type", StringType())
]


def read_categories(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/raw/{date}/categories")


def read_stations(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return spark.read.json(f"{bucket_path}/raw/{date}/categories", schema=StructType(station_fields))


def read_timeseries(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/raw/{date}/timeseries")


def read_timeseriesdata(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    return __read_from_s3(spark, f"{bucket_path}/raw/{date}/timeseriesdata")


def __read_from_s3(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.json(path)
