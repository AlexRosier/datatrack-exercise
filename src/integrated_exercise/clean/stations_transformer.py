from pyspark.sql import DataFrame, Row, Column
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType
)
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="Datatrack-Alex")


def transform(df_stations: DataFrame, date: str) -> DataFrame:
    df_exploded_stations = df_stations.select(
        psf.col("properties.id").alias("station_id"),
        psf.col("properties.label").alias("station_label"),
        psf.col("geometry"),
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
        "station_coordinates_x": psf.col("geometry.coordinates").getItem(0),
        "station_coordinates_y": psf.col("geometry.coordinates").getItem(1),
        "station_coordinates_z": psf.col("geometry.coordinates").getItem(2)
    })

    df_renamed_enriched = __enrich_with_geo_info(df_renamed)
    df_dropped = (df_renamed_enriched
                  .withColumn("ds", psf.lit(date))
                  .drop("timeseries")
                  .drop("geometry")
                  .drop("station"))

    return df_dropped.replace(float("NaN"), None)

def __enrich_with_geo_info(dataframe: DataFrame) -> DataFrame:
    udf_enrich_with_geo_info = __create_geo_enrich_udf()

    df_enriched = dataframe.withColumn("station", udf_enrich_with_geo_info(psf.col("station_coordinates_x"), psf.col("station_coordinates_y")))
    return df_enriched.withColumns({
        "station_city": psf.col("station.city"),
        "station_state": psf.col("station.state"),
        "station_country": psf.col("station.country")
    })


def __create_geo_enrich_udf() -> psf.udf:
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType())
    ])

    return psf.udf(__get_geo_info, schema)


def __get_geo_info(x_coordinate: float, y_coordinate: float) -> Row:
    query = f"{x_coordinate}, {y_coordinate}"
    response = geolocator.reverse(query, language="en")
    address = response.raw['address']
    return Row('city', 'state', 'country')(address.get('city', None), address.get('state', None), address.get('country', None))
