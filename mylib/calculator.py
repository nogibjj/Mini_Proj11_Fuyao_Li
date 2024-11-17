import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     FloatType, 
     StringType, 
)


LOG_FILE = "pyspark_log.md"


def log_output(operation, output=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if output is not None:
            file.write("The truncated output is: \n\n")
            file.write(output)
            file.write("\n\n")


def create_spark_session(appName):
    """Create and return a Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    log_output("Spark session created")
    return spark


def extract(
        url1="https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true", 
        file_path1="data/trump.csv",
    ):
    """Extract urls to a file path"""
    os.makedirs(os.path.dirname(file_path1), exist_ok=True)

    response1 = requests.get(url1)
    if response1.status_code == 200:
        with open(file_path1, "wb") as f:
            f.write(response1.content)
        print(f"File successfully downloaded to {file_path1}")
    else:
        print(
            f"Failed to retrieve the file from {url1}."
        )

    return file_path1


def load(spark, data="data/trump.csv"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("location", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True)
        
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(5).toPandas().to_markdown())

    return df


def query_data(spark, df):
    df.createOrReplaceTempView("trump")
    result = spark.sql("SELECT * FROM trump WHERE lat > 40")
    log_output("Query data", result.limit(10).toPandas().to_markdown())
    return result


def transform(df):
    # Create the 'Region' column using when() conditions
    transform_df = df.withColumn(
        "Region",
        when(col("lng") < -100, "West")
        .when((col("lng") < -80) & (col("lng") >= -100), "Central")
        .when(col("lng") >= -80, "East")
    )

    # Log the output of the first 10 rows to check the transformation
    log_output("transform data", transform_df.limit(10).toPandas().to_markdown())

    return transform_df
