import logging

from pyspark.sql import DataFrame


# Shamelessly stolen (and improved) code as this not a real production project
# Source : https://github.com/datamindedacademy/effective_pyspark/blob/main/tests/comparers.py
def assert_frames_functionally_equivalent(
        actual: DataFrame, expected: DataFrame, check_nullability=True
):
    """
    Validate if two non-nested dataframes have identical schemas, and data,
    ignoring the ordering of both.
    """
    # This is what we call an “early-out”: here it is computationally cheaper
    # to validate that two things are not equal, rather than finding out that
    # they are equal.
    try:
        if check_nullability:
            assert set(actual.schema.fields) == set(expected.schema.fields)
        else:
            assert set(actual.dtypes) == set(expected.dtypes)
    except AssertionError:
        print("Schema does not match")
        print("Expected schema : ")
        expected.printSchema()
        print("Actual schema : ")
        actual.printSchema()
        print("Difference schema: " + str(set(actual.schema.fields) - set(expected.schema.fields)))
        logging.warning(actual.schema)
        logging.warning(expected.schema)
        raise

    try:
        sorted_rows_expected = expected.select(actual.columns).orderBy(actual.columns).collect()
        sorted_rows_actual = actual.orderBy(*actual.columns).collect()
        assert sorted_rows_expected == sorted_rows_actual
    except AssertionError:
        print("Data does not match")
        print(f"Expected : {sorted_rows_expected}")
        print(f"Actual : {sorted_rows_actual}")
        print("Difference data : " + str(set(sorted_rows_expected) - set(sorted_rows_actual)))
        raise
