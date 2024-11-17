"""
Test goes here

"""
import os
import pytest
from mylib.calculator import (
    create_spark_session,
    extract,
    load,
    query_data,
    transform
)

@pytest.fixture(scope="module")
def spark():
    spark = create_spark_session("TestApp")
    yield spark
    spark.stop()


def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True


def test_load(spark):
    df = load(spark)
    assert df is not None


def test_query(spark):
    df = load(spark)
    result = query_data(
        spark, df
    )
    assert result is not None


def test_transform(spark):
    df = load(spark)
    result = transform(df)
    assert result is not None


if __name__ == "__main__":
    spark()
    test_extract()
    test_load(spark)
    test_query(spark)
    test_transform(spark)
