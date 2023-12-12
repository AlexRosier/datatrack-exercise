from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
import logging
import argparse
import sys

import src.integrated_exercise.aggregate.base_aggregation as base_aggregation
import src.integrated_exercise.aggregate.aggregate_writer as aggregate_writer

stations = 'stations'
timeseriesdata = 'timeseriesdata'

spark = SparkSession.builder.config(
    "spark.jars.packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.1",
            "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
            "net.snowflake:snowflake-jdbc:3.13.3"
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()


def pollution_per_city_and_category(dataframe: DataFrame) -> DataFrame:
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

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.info("Entering aggregate main")
    parser = argparse.ArgumentParser(description="Pyspark aggregate")
    parser.add_argument(
        "-d", "--date", dest="date", help="Date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-b", "--bucket_path", dest="bucket_path", help="Path on S3 to the bucket first sub folder", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")

    df_base_aggregation = base_aggregation.execute(spark, args.bucket_path, args.date)

    df_pollution_per_city_and_category = pollution_per_city_and_category(df_base_aggregation)
    aggregate_writer.write_dataframe(df_pollution_per_city_and_category, spark, args.bucket_path, args.date, "pollution_per_city_and_category")

    df_stations_per_city = stations_per_city(df_base_aggregation)
    aggregate_writer.write_dataframe(df_stations_per_city, spark, args.bucket_path, args.date, "stations_per_city")


if __name__ == "__main__":
    main()