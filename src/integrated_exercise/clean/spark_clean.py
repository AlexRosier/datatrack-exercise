from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging
import argparse
import sys
import raw_reader

import categories_transformer
import stations_transformer
import timeseries_transformer
import timeseriesdata_transformer

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

categories = 'categories'
stations = 'stations'
timeseries = 'timeseries'
timeseriesdata = 'timeseriesdata'


def read(bucket_path: str, date: str) -> dict[str, DataFrame]:
    df_categories = raw_reader.read_categories(spark, bucket_path, date)
    df_stations = raw_reader.read_stations(spark, bucket_path, date)
    df_timeseries = raw_reader.read_timeseries(spark, bucket_path, date)
    df_timeseriesdata = raw_reader.read_timeseriesdata(spark, bucket_path, date)

    return {categories: df_categories, stations: df_stations, timeseries: df_timeseries,
            timeseriesdata: df_timeseriesdata}


def transform_dataframes(dataframes: dict[str, DataFrame], date: str) -> dict[str, DataFrame]:
    df_categories = categories_transformer.transform(dataframes.get(categories), date)
    df_stations = stations_transformer.transform(dataframes.get(stations), date)
    df_timeseries = timeseries_transformer.transform(dataframes.get(timeseries), date)
    df_timeseriesdata = timeseriesdata_transformer.transform(dataframes.get(timeseriesdata), date)

    return {categories: df_categories, stations: df_stations, timeseries: df_timeseries,
            timeseriesdata: df_timeseriesdata}


def write_dataframes(dataframes: dict[str, DataFrame], bucket_path: str, date: str):
    for key in dataframes.keys():
        dataframes.get(key).write.parquet(f"{bucket_path}/source/{date}/{key}/", mode="overwrite")


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.info("Entering clean main")
    parser = argparse.ArgumentParser(description="Pyspark clean")
    parser.add_argument(
        "-d", "--date", dest="date", help="Date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-b", "--bucket_path", dest="bucket_path", help="Path on S3 to the bucket first sub folder", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")

    dataframes = read(args.bucket_path, args.date)
    transformed_dataframes = transform_dataframes(dataframes, args.date)
    write_dataframes(transformed_dataframes, args.bucket_path, args.date)


if __name__ == "__main__":
    main()