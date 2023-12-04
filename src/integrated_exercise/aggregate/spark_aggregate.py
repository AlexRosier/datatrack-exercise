from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
import logging
import argparse
import sys

import src.integrated_exercise.aggregate.source_reader as source_reader

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


def read(bucket_path: str, date: str) -> dict[str, DataFrame]:
    df_stations = source_reader.read_stations(spark, bucket_path, date)
    df_timeseriesdata = source_reader.read_timeseriesdata(spark, bucket_path, date)

    return {stations: df_stations, timeseriesdata: df_timeseriesdata}


def transform(dataframes: dict[str, DataFrame]) -> dict[str, DataFrame]:
    df_timeseriesdata_transformed = (dataframes.get(timeseriesdata)
                                     .select(psf.col("timeseriesdata_id"), psf.col("value"))
                                     .groupBy(psf.col('timeseriesdata_id')).agg(psf.avg("value").alias("average_value")))

    dataframes.update({timeseriesdata: df_timeseriesdata_transformed})
    return dataframes


def join(dataframes: dict[str, DataFrame]) -> DataFrame:
    df_stations = dataframes.get(stations)
    df_timeseriesdata = dataframes.get(timeseriesdata)
    logging.info("station columns : " + str(df_stations.columns))
    df_joined = df_stations.join(df_timeseriesdata, df_stations.timeseries_id == df_timeseriesdata.timeseriesdata_id, "leftouter")
    return df_joined.drop(psf.col("timeseriesdata_id"))


def write(dataframe: DataFrame, bucket_path: str, date: str):
    dataframe.write.partitionBy("station_category_id").parquet(f"{bucket_path}/derived/{date}/stations_average_value/", mode="overwrite")


def execute_base_aggregation(dataframes: dict[str, DataFrame]) -> DataFrame:
    dataframes_transformed = transform(dataframes)
    return join(dataframes_transformed)


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
    dataframe = execute_base_aggregation(dataframes)
    write(dataframe, args.bucket_path, args.date)


if __name__ == "__main__":
    main()