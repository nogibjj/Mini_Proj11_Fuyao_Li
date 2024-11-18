from mylib.extract import extract
from mylib.transform import transform
from mylib.query import query
from mylib.load import load_to_databricks
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # Main ETL process
    
    spark = SparkSession.builder.appName('PySparkTrump').getOrCreate()

    # Extract
    extract()

    # Load
    load_to_databricks()

    # Query
    query()

    # Transform
    transform()

    # Stop the Spark session
    spark.stop()
