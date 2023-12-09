from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
import logging
import argparse
import sys

import src.integrated_exercise.aggregate.base_aggregation as base_aggregation

stations = 'stations'
timeseriesdata = 'timeseriesdata'

spark = SparkSession.builder.config(
    "spark.jars.packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.1"
            # "net.snowflake:spark-snowflake_2.13.0-spark_3.4",
            # "net.snowflake:snowflake-jdbc:3.14.4"
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()


def most_polluted_pm10(dataframe: DataFrame) -> DataFrame:
    return (dataframe
            .groupBy(psf.col('station_native_city'), psf.col("station_category_id"), psf.col("ds"))
            .agg(psf.avg("average_value").alias("city_average_value"))
            .sort(psf.col("city_average_value"), ascending=False))


def stations_per_city(dataframe: DataFrame) -> DataFrame:

    dataframe_deduplicated = dataframe.dropDuplicates(["station_id"])
    return (dataframe_deduplicated
            .groupBy(psf.col('station_geopy_county'), psf.col('station_geopy_city'), psf.col('station_geopy_state'), psf.col("ds"))
            .agg(psf.count("station_id").alias("number_of_stations"))
            .sort(psf.col("number_of_stations"), ascending=False))


def __write(dataframe: DataFrame, bucket_path: str, date: str, key: str):
    dataframe.write.parquet(f"{bucket_path}/derived/{date}/{key}/", mode="overwrite")


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.info("Entering aggregate main")
    parser = argparse.ArgumentParser(description="Pyspark clean")
    parser.add_argument(
        "-d", "--date", dest="date", help="Date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-b", "--bucket_path", dest="bucket_path", help="Path on S3 to the bucket first sub folder", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")
    df_base_aggregation = base_aggregation.execute(spark, args.bucket_path, args.date)

    df_most_polluted_pm10 = most_polluted_pm10(df_base_aggregation)
    __write(df_most_polluted_pm10, args.bucket_path, args.date, "most_polluted_pm10")

    df_stations_per_city = stations_per_city(df_base_aggregation)
    __write(df_stations_per_city, args.bucket_path, args.date, "stations_per_city")


if __name__ == "__main__":
    main()