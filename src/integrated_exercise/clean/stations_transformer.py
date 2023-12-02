from pyspark.sql import DataFrame
import pyspark.sql.functions as psf


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

    df_dropped = (df_renamed
                  .withColumn("ds", psf.lit(date))
                  .drop("timeseries")
                  .drop("geometry"))

    return df_dropped.replace(float("NaN"), None)