from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf

import src.integrated_exercise.aggregate.source_reader as source_reader
import src.integrated_exercise.aggregate.aggregate_writer as aggregate_writer

stations = 'stations'
timeseriesdata = 'timeseriesdata'


def __read(spark: SparkSession, bucket_path: str, date: str) -> dict[str, DataFrame]:
    df_stations = source_reader.read_stations(spark, bucket_path, date)
    df_timeseriesdata = source_reader.read_timeseriesdata(spark, bucket_path, date)

    return {stations: df_stations, timeseriesdata: df_timeseriesdata}


def __transform(dataframes: dict[str, DataFrame]) -> dict[str, DataFrame]:
    df_timeseriesdata_transformed = (dataframes.get(timeseriesdata)
                                     .select(psf.col("timeseriesdata_id"), psf.col("value"))
                                     .groupBy(psf.col('timeseriesdata_id')).agg(psf.avg("value").alias("average_value")))

    dataframes.update({timeseriesdata: df_timeseriesdata_transformed})
    return dataframes


def __join(dataframes: dict[str, DataFrame]) -> DataFrame:
    df_stations = dataframes.get(stations)
    df_timeseriesdata = dataframes.get(timeseriesdata)
    df_joined = df_stations.join(df_timeseriesdata, df_stations.timeseries_id == df_timeseriesdata.timeseriesdata_id, "leftouter")
    return df_joined.drop(psf.col("timeseriesdata_id"))


def _execute_base_aggregation(dataframes: dict[str, DataFrame]) -> DataFrame:
    dataframes_transformed = __transform(dataframes)
    return __join(dataframes_transformed).cache()


def execute(spark: SparkSession, bucket_path: str, date: str) -> DataFrame:
    dataframes = __read(spark, bucket_path, date)
    dataframes_transformed = __transform(dataframes)
    dataframe = __join(dataframes_transformed).cache()
    aggregate_writer.write_dataframe(dataframe, spark, bucket_path, date, "stations_average_value")
    return dataframe