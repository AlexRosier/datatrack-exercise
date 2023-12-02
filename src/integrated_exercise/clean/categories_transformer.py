from pyspark.sql import DataFrame
import pyspark.sql.functions as psf


def transform(categories: DataFrame, date: str) -> DataFrame:
    return (categories
            .withColumnRenamed("id", "category_id")
            .withColumn("ds", psf.lit(date))
            )
