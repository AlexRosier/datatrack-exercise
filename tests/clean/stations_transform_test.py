from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
)
from tests.comparers import assert_frames_functionally_equivalent
from src.integrated_exercise.clean.raw_reader import station_fields
from src.integrated_exercise.clean import stations_transformer

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_stations = spark.read.option("multiline", "true").json("data/stations.json", schema=StructType(station_fields))


def test_transform():
    date = "2023-12-02"
    df_transformed = stations_transformer.transform(df_stations, date)

    fields = [
        StructField("station_id", IntegerType()),
        StructField("station_label", StringType()),
        StructField("station_type", StringType()),
        StructField("timeseries_id", StringType(), False),
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
        StructField("station_coordinates_lon", FloatType()),
        StructField("station_coordinates_lat", FloatType()),
        StructField("station_coordinates_z", FloatType(), True),
        StructField("station_coordinates_type", StringType()),
        StructField("station_native_city", StringType()),
        StructField("station_geopy_postal_code", StringType()),
        StructField("station_geopy_county", StringType()),
        StructField("station_geopy_city", StringType()),
        StructField("station_geopy_state", StringType()),
        StructField("station_geopy_region", StringType()),
        StructField("station_geopy_country", StringType()),
        StructField("station_native_city_lon", FloatType()),
        StructField("station_native_city_lat", FloatType()),
        StructField("ds", StringType(), False),
    ]
    df_expected = spark.createDataFrame(
        [
            (1030, "40AL01 - Linkeroever", "Feature", 6151, "1", "IRCEL - CELINE: timeseries-api (SOS 2.0)", "6151",
             "6151 - Unknown device - procedure", "1030", "40AL01 - Linkeroever", "6151",
             "6151 - Unknown device - procedure", "391", "Black Carbon", "391", "Black Carbon",  4.385223684454717, 51.23619419990248,
             None, "Point", "Linkeroever", "2050", "Antwerp", "Antwerp", "Antwerp", "Flanders", "Belgium", float(4.386194705963135), float(51.22365951538086), date),
            (1030, "40AL01 - Linkeroever", "Feature", 6152, "1", "IRCEL - CELINE: timeseries-api (SOS 2.0)", "6152",
             "6152 - DAILY CORRECTION TEOM - procedure", "1030", "40AL01 - Linkeroever", "6152",
             "6152 - DAILY CORRECTION TEOM - procedure", "5", "Particulate Matter < 10", "5",
             "Particulate Matter < 10", 4.385223684454717, 51.23619419990248, None, "Point", "Linkeroever", "2050", "Antwerp", "Antwerp", "Antwerp", "Flanders", "Belgium", float(4.386194705963135), float(51.22365951538086), date),
            (1031, "40AL02 - Beveren (some further indication)", "Feature", 6153, "1", "IRCEL - CELINE: timeseries-api (SOS 2.0)", "6153",
             "6153 - Unknown device - procedure", "1031", "40AL02 - Beveren", "6153",
             "6153 - Unknown device - procedure", "5", "Particulate Matter < 10", "5", "Particulate Matter < 10",
             4.234832753144059, 51.30452079034428, None, "Point", "Beveren", "9130", "Sint-Niklaas", None, "East Flanders", None, "Belgium", float(4.257781028747559), float(51.21261215209961), date)
        ],
        schema=StructType(fields),
    )

    assert_frames_functionally_equivalent(df_transformed, df_expected, False)
