from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from tests.comparers import assert_frames_functionally_equivalent

from src.integrated_exercise.clean import categories_transformer

spark = SparkSession.builder.master("local[*]").getOrCreate()
df_categories = spark.read.json("data/categories.json")


def test_transform():
    date = "2023-12-02"
    df_transformed = categories_transformer.transform(df_categories, date)

    fields = [
        StructField("category_id", StringType()),
        StructField("label", StringType()),
        StructField("ds", StringType(), False)
    ]
    expected = spark.createDataFrame(
        [
            ("482", "1,2-XYLENE O-XYLENE", date),
            ("35", "Ammonia", date),
            ("64102", "Atmospheric  Pressure", date)
        ],
        schema=StructType(fields),
    )

    assert_frames_functionally_equivalent(df_transformed, expected, False)
