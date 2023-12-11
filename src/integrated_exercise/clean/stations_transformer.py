from pyspark.sql import DataFrame, Row, Column
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    FloatType
)
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="Datatrack-Alex")


def transform(df_stations: DataFrame, date: str) -> DataFrame:
    df_enriched = __enrich_with_geo_info(df_stations)
    df_native_city = df_enriched.withColumn("station_native_city", psf.split(psf.col("properties.label"), r'\s-\s|\s\(').getItem(1))
    df_enriched_native_coordinates = __enrich_with_coordinates(df_native_city)

    df_exploded_stations = df_enriched_native_coordinates.select(
        psf.col("properties.id").alias("station_id"),
        psf.col("properties.label").alias("station_label"),
        psf.col("station_native_city"),
        psf.col("geometry"),
        psf.col("station.*"),
        psf.col("native_city.*"),
        psf.col("geometry.type").alias("station_coordinates_type"),
        psf.col("type").alias('station_type'),
        psf.explode("properties.timeseries").alias("timeseries_id", "timeseries"))

    df_renamed = df_exploded_stations.withColumns({
        "station_services_id": psf.col("timeseries.service.id"),
        "station_services_label": psf.col("timeseries.service.label"),
        "station_offering_id": psf.col("timeseries.offering.id"),
        "station_offering_label": psf.col("timeseries.offering.label"),
        "station_feature_id": psf.col("timeseries.feature.id"),
        "station_feature_label": psf.col("timeseries.feature.label"),
        "station_procedure_id": psf.col("timeseries.procedure.id"),
        "station_procedure_label": psf.col("timeseries.procedure.label"),
        "station_phenomenon_id": psf.col("timeseries.phenomenon.id"),
        "station_phenomenon_label": psf.col("timeseries.phenomenon.label"),
        "station_category_id": psf.col("timeseries.category.id"),
        "station_category_label": psf.col("timeseries.category.label"),
        "station_coordinates_lon": psf.col("geometry.coordinates").getItem(0),
        "station_coordinates_lat": psf.col("geometry.coordinates").getItem(1),
        "station_coordinates_z": psf.col("geometry.coordinates").getItem(2)
    })

    df_dropped = (df_renamed
                  .withColumn("ds", psf.lit(date))
                  .drop("timeseries")
                  .drop("geometry")
                  .drop("station"))

    return df_dropped.replace(float("NaN"), None)


def __enrich_with_coordinates(dataframe: DataFrame) -> DataFrame:
    udf_enrich_with_coordinates = __create_coordinates_enrich_udf()
    return dataframe.withColumn("native_city", udf_enrich_with_coordinates(psf.col("station_native_city")))


def __create_coordinates_enrich_udf() -> psf.udf:
    schema = StructType([
        StructField("station_native_city_lon", FloatType()),
        StructField("station_native_city_lat", FloatType()),
    ])

    return psf.udf(__get_coordinates, schema)


def __get_coordinates(native_city: str) -> Row:
    try:
        response = geolocator.geocode(native_city, language="en")
        return Row('station_native_city_lon', 'station_native_city_lat')(
            response.longitude,
            response.latitude)

        return row
    except:
        return Row('station_native_city_lon', 'station_native_city_lat')(None, None)


def __enrich_with_geo_info(dataframe: DataFrame) -> DataFrame:
    udf_enrich_with_geo_info = __create_geo_enrich_udf()
    return dataframe.withColumn("station", udf_enrich_with_geo_info(psf.col("geometry.coordinates").getItem(1), psf.col("geometry.coordinates").getItem(0)))


def __create_geo_enrich_udf() -> psf.udf:
    schema = StructType([
        StructField("station_geopy_postal_code", StringType()),
        StructField("station_geopy_county", StringType()),
        StructField("station_geopy_city", StringType()),
        StructField("station_geopy_state", StringType()),
        StructField("station_geopy_region", StringType()),
        StructField("station_geopy_country", StringType()),
    ])

    return psf.udf(__get_geo_info, schema)


def __get_geo_info(coordinate_lat: float, coordinate_lon: float) -> Row:
    try:
        query = f"{coordinate_lat}, {coordinate_lon}"
        response = geolocator.reverse(query, language="en")
        address = response.raw['address']
        return Row('station_geopy_postal_code', 'station_geopy_county', 'station_geopy_city', 'station_geopy_state', 'station_geopy_region', 'station_geopy_country')(
            address.get('postcode', None),
            address.get('county', None),
            address.get('city', None),
            address.get('state', None),
            address.get('region', None),
            address.get('country', None))

    except:
        return Row('station_geopy_postal_code', 'station_geopy_county', 'station_geopy_city', 'station_geopy_state', 'station_geopy_region', 'station_geopy_country')(None, None, None, None, None,None)