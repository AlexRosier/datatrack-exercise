from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    LongType,
    StringType

)


def transform(df_timeseriesdata: DataFrame, date: str) -> DataFrame:
    df_timeseriesdata_exploded = df_timeseriesdata.select(psf.col("timeseriesdata_id").cast(IntegerType()),
                                                          psf.explode("values").alias("exploded_values"))

    df_added_columns = (df_timeseriesdata_exploded.withColumns({
        "epoch_milliseconds": psf.col("exploded_values.timestamp").cast(LongType()),
        "value": psf.col("exploded_values.value").cast(FloatType())
    })
                        .withColumn("epoch_seconds", psf.col("epoch_milliseconds") / 1000)
                        .withColumn("timestamp", psf.to_utc_timestamp(psf.from_unixtime(psf.col("epoch_seconds"), 'yyyy-MM-dd HH:mm:ss'), 'CET').cast(StringType()))
                        .withColumn("timezone", psf.lit("CET"))
                        .withColumn("ds", psf.lit(date)))

    return (df_added_columns
            .select(psf.col("timeseriesdata_id"), psf.col("epoch_milliseconds"), psf.col("value"), psf.col("epoch_seconds").cast(LongType()), psf.col("timestamp"), psf.col("timezone"), psf.col("ds"))
            .replace(float("NaN"), None))
