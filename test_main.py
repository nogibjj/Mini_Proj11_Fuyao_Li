"""
Test goes here

"""
from mylib.extract import extract
from pyspark.sql import SparkSession


def test_extract():
    file_path = extract()
    assert file_path is not None


if __name__ == "__main__":
    spark = SparkSession.builder.appName('PySparkTrump').getOrCreate()
    test_extract()
