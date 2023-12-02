from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    MapType,
    ArrayType
)

category_fields = [
    StructField("properties", StructType(fields=[
        StructField("id", IntegerType()),
        StructField("label", StringType()),
        StructField("timeseries", MapType(StringType(), MapType(StringType(), StructType(fields=[
            StructField("id", StringType()), StructField("label", StringType())
        ]))))
    ])),
    StructField("geometry", StructType(fields=[
        StructField("coordinates", ArrayType(FloatType())),
        StructField("type", StringType())
    ])),
    StructField("type", StringType())
]


def read_categories(spark: SparkSession, bucket_read_path: str, date: str) -> DataFrame:
    return spark.read.json(f"{bucket_read_path}/{date}/categories", schema=StructType(category_fields))
