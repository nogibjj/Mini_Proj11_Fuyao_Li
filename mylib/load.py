from pyspark.sql import SparkSession

from pyspark.sql.types import (
     StructType, 
     StructField, 
     FloatType, 
     StringType, 
)

def create_spark_session(appName):
    """Create and return a Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def load_to_databricks(
    spark, 
    data="data/trump.csv",
    catalog="ids706_data_engineering",
    database="fuyao_db",
    table_name="rally",
):
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("location", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True)  
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
    table_full_name = f"{catalog}.{database}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(table_full_name)

    print(f"Table successfully created: {table_full_name}")


if __name__ == "__main__":
    load_to_databricks()