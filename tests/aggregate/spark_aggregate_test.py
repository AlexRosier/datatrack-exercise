from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.aggregate import spark_aggregate

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_stations = spark.read.option("multiline", "true").json("data/stations.json")
df_timeseriesdata = spark.read.option("multiline", "true").json("data/timeseriesdata.json")


def test_execute_base_aggregation():
    df_aggregated = spark_aggregate.execute_base_aggregation({spark_aggregate.stations: df_stations, spark_aggregate.timeseriesdata: df_timeseriesdata})

    fields = [
        StructField("station_id", LongType()),
        StructField("station_label", StringType()),
        StructField("station_type", StringType()),
        StructField("timeseries_id", StringType()),
        StructField("station_services_id", StringType()),
        StructField("station_services_label", StringType()),
        StructField("station_offering_id", StringType()),
        StructField("station_offering_label", StringType()),
        StructField("station_feature_id", StringType()),
        StructField("station_feature_label", StringType()),
        StructField("station_procedure_id", StringType()),
        StructField("station_procedure_label", StringType()),
        StructField("station_phenomenon_id", StringType()),
        StructField("station_phenomenon_label", StringType()),
        StructField("station_category_id", StringType()),
        StructField("station_category_label", StringType()),
        StructField("station_coordinates_x", DoubleType()),
        StructField("station_coordinates_y", DoubleType()),
        StructField("station_coordinates_z", StringType(), True),
        StructField("station_coordinates_type", StringType()),
        StructField("average_value", DoubleType()),
        StructField("ds", StringType(), False)
    ]
    df_expected = spark.read.option("multiline", "true").json("data/expected/base_aggregation.json",  schema=StructType(fields))

    assert_frames_functionally_equivalent(df_aggregated, df_expected, False)
