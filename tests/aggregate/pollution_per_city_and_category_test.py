from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.aggregate import base_aggregation
from src.integrated_exercise.aggregate import spark_aggregate

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_stations = spark.read.option("multiline", "true").json("data/stations.json")
df_timeseriesdata = spark.read.option("multiline", "true").json("data/timeseriesdata.json")


def test_most_polluted_pm10():
    df_aggregated = base_aggregation._execute_base_aggregation({base_aggregation.stations: df_stations, base_aggregation.timeseriesdata: df_timeseriesdata})
    df_most_polluted = spark_aggregate.pollution_per_city_and_category(df_aggregated)

    fields = [
        StructField("station_native_city", StringType()),
        StructField("station_category_id", StringType()),
        StructField("city_average_value", DoubleType(), False),
        StructField("ds", StringType(), False),
    ]
    df_expected = spark.read.option("multiline", "true").json("data/expected/pollution_per_city_and_category.json", schema=StructType(fields))

    assert_frames_functionally_equivalent(df_most_polluted, df_expected, False)
