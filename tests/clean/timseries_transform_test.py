from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.clean import timeseries_transformer

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_timeseries = spark.read.option("multiline", "true").json("data/timeseries.json")


def test_transform():
    date = "2023-12-02"
    df_transformed = timeseries_transformer.transform(df_timeseries, date)

    fields = [
        StructField("timeseries_id", IntegerType()),
        StructField("timeseries_label", StringType()),
        StructField("timeseries_uom", StringType()),
        StructField("timeseries_station_id", IntegerType()),
        StructField("timeseries_station_label", StringType()),
        StructField("timeseries_station_type", StringType()),
        StructField("timeseries_station_coordinates_x", FloatType()),
        StructField("timeseries_station_coordinates_y", FloatType()),
        StructField("timeseries_station_coordinates_z", FloatType(), True),
        StructField("timeseries_station_coordinates_type", StringType()),
        StructField("ds", StringType(), False)
    ]
    df_expected = spark.createDataFrame(
        [
            (6522, "1,2-XYLENE O-XYLENE 6522 - btx, o-xyleen - procedure, 41B006 - Bruxelles (Parlement UE)", "5S", 1112, "41B006 - Bruxelles (Parlement UE)", "Feature", 4.374388284562104, 50.838640177166184, None, "Point", date),
            (10892, "Ammonia 10892 - MINI DOAS - procedure, 42BO01 - Bonheiden", "5S", 1738, "42BO01 - Bonheiden", "Feature", 4.516483717002172, 51.02706104655742, None, "Point", date),
        ],
        schema=StructType(fields),
    )

    assert_frames_functionally_equivalent(df_transformed, df_expected, False)