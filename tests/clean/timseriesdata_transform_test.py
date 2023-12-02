from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    LongType,
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.clean import timeseriesdata_transformer

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_timeseries = spark.read.option("multiline", "true").json("data/timeseriesdata.json")


def test_transform():
    date = "2023-12-02"
    df_transformed = timeseriesdata_transformer.transform(df_timeseries, date)

    fields = [
        StructField("timeseriesdata_id", IntegerType()),
        StructField("epoch_milliseconds", LongType()),
        StructField("value", FloatType()),
        StructField("epoch_seconds", LongType()),
        StructField("timestamp", StringType()),
        StructField("timezone", StringType()),
        StructField("ds", StringType(), False)
    ]
    df_expected = spark.createDataFrame(
        [
            (6906, 1701133200000, 13.5, 1701133200, "2023-11-28 01:00:00", "CET", date),
            (6906, 1701136800000, 12.5, 1701136800, "2023-11-28 02:00:00", "CET", date),
            (6906, 1701140400000, 7.5, 1701140400, "2023-11-28 03:00:00", "CET", date),
            (10897, 1701133200000, None, 1701133200, "2023-11-28 01:00:00", "CET", date),
            (10897, 1701136800000, None, 1701136800, "2023-11-28 02:00:00", "CET", date),
            (10897, 1701169200000, 0.315, 1701169200, "2023-11-28 11:00:00", "CET", date),
            (10897, 1701172800000, 0.35, 1701172800, "2023-11-28 12:00:00", "CET", date),
        ],
        schema=StructType(fields),
    )

    assert_frames_functionally_equivalent(df_transformed, df_expected, False)
