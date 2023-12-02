from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
import os
import logging
import argparse
import sys

spark = SparkSession.builder.config(
    "spark.jars.packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.1",
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()


def read(date: str) -> dict:
    bucket = os.getenv("bucket")
    df_categories = spark.read.option("multiline", "true").json(f"s3a://{bucket}/alex-data/{date}/categories")
    df_stations = spark.read.option("multiline", "true").json(f"s3a://{bucket}/alex-data/{date}/stations")
    df_timeseries = spark.read.option("multiline", "true").json(f"s3a://{bucket}/alex-data/{date}/timeseries")
    df_timeseriesdata = spark.read.option("multiline", "true").json(f"s3a://{bucket}/alex-data/{date}/timeseriesdata")

    return {'categories': df_categories, 'stations': df_stations, 'timesseries': df_timeseries,
            'timeseriesdata': df_timeseriesdata}


def transform_dataframes(dataframes: dict, date: str) -> dict:
    df_categories = transform(dataframes.get("categories"), date)
    df_stations = transform(dataframes.get("stations"), date)
    df_timeseries = transform(dataframes.get("timeseries"), date)
    df_timeseriesdata = transform_timeseries_data(dataframes.get("timeseriesdata"), date)

    return {'categories': df_categories, 'stations': df_stations, 'timesseries': df_timeseries,
            'timeseriesdata': df_timeseriesdata}


def transform(dataframe: DataFrame, date: str) -> DataFrame:
    return dataframe.withColumn("ds", psf.lit(date))


def transform_timeseries_data(df_timeseries_data: DataFrame, date: str) -> DataFrame:
    return (df_timeseries_data
            .withColumn("timeseries_timestamp_normalized", psf.to_utc_timestamp(
        psf.from_unixtime(psf.col("timeseries_data_timestamp") / 1000, 'yyyy-MM-dd HH:mm:ss'), 'CET'))
            .withColumn("ds", psf.lit(date)))


def write_dataframes(dataframes: dict, date: str):
    bucket = os.getenv("bucket")
    for key, value in dataframes:
        value.repartition(1).write.parquet(f"s3a://{bucket}/alex-data/clean/{date}/{key}", mode="overwrite")


def main():
    logging.info("Entering pyspark main")
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser(description="Pyspark clean")
    parser.add_argument(
        "-d", "--date", dest="date", help="Date in format YYYY-mm-dd", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")

    dataframes = read(args.date)
    transformed_dataframes = transform_dataframes(dataframes, args.date)
    write_dataframes(transformed_dataframes, args.date)


if __name__ == "__main__":
    main()