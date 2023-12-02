from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    IntegerType,
    FloatType
)


def transform(df_timeseries: DataFrame, date: str) -> DataFrame:
    df_moved = df_timeseries.withColumns({
        "timeseries_station_id": psf.col("station.properties.id").cast(IntegerType()),
        "timeseries_station_label": psf.col("station.properties.label"),
        "timeseries_station_type": psf.col("station.type"),
        "timeseries_station_coordinates_x": psf.col("station.geometry.coordinates").getItem(0).cast(FloatType()),
        "timeseries_station_coordinates_y": psf.col("station.geometry.coordinates").getItem(1).cast(FloatType()),
        "timeseries_station_coordinates_z": psf.col("station.geometry.coordinates").getItem(2).cast(FloatType()),
        "timeseries_station_coordinates_type": psf.col("station.geometry.type")
    })

    df_renamed = df_moved.withColumnsRenamed({
        "id": "timeseries_id",
        "label": "timeseries_label",
        "uom": "timeseries_uom"
    }).withColumn("timeseries_id", psf.col("timeseries_id").cast(IntegerType()))

    df_dropped = df_renamed.drop(psf.col("station"))

    return (df_dropped
            .withColumn("ds", psf.lit(date))
            .replace(float("NaN"), None))
