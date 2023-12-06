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
            "org.apache.hadoop:hadoop-aws:3.3.1",
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()


def most_polluted_pm10(dataframe: DataFrame) -> DataFrame:
    return (dataframe
            .groupBy(psf.col('station_county'), psf.col('station_city'), psf.col('station_state'), psf.col("station_category_id"), psf.col("ds"))
            .agg(psf.avg("average_value").alias("city_average_value"))
            .sort(psf.col("city_average_value"), ascending=False))


def __write(dataframe: DataFrame, bucket_path: str, date: str):
    dataframe.write.parquet(f"{bucket_path}/derived/{date}/most_polluted_pm10/", mode="overwrite")


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
    __write(df_most_polluted_pm10, args.bucket_path, args.date)


if __name__ == "__main__":
    main()
