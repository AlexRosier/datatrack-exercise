from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    LongType
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.aggregate import base_aggregation
from src.integrated_exercise.aggregate import spark_aggregate

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_stations = spark.read.option("multiline", "true").json("data/stations.json")
df_timeseriesdata = spark.read.option("multiline", "true").json("data/timeseriesdata.json")


def test_stations_per_city():
    df_aggregated = base_aggregation._execute_base_aggregation({base_aggregation.stations: df_stations, base_aggregation.timeseriesdata: df_timeseriesdata})
    df_stations_per_city = spark_aggregate.stations_per_city(df_aggregated)

    fields = [
        StructField("station_county", StringType()),
        StructField("station_city", StringType()),
        StructField("station_state", StringType()),
        StructField("number_of_stations", LongType(), False),
        StructField("ds", StringType(), False),
    ]
    df_expected = spark.read.option("multiline", "true").json("data/expected/stations_per_city.json", schema=StructType(fields))

    assert_frames_functionally_equivalent(df_stations_per_city, df_expected, False)
