from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


spark = SparkSession.builder.appName("Transform").getOrCreate()


def transform(
    catalog="ids706_data_engineering",
    input_database="fuyao_db",
    input_table_name="rally",
    output_database="fuyao_db_transform",
    output_table_name="rally_region",
):
    input_table_full_name = f"{catalog}.{input_database}.{input_table_name}"
    output_table_full_name = f"{catalog}.{output_database}.{output_table_name}"

    print(f"Reading input table from: {input_table_full_name}")
    df = spark.table(input_table_full_name)

    # Create the 'Region' column using when() conditions
    transform_df = df.withColumn(
        "Region",
        when(col("lng") < -100, "West")
        .when((col("lng") < -80) & (col("lng") >= -100), "Central")
        .when(col("lng") >= -80, "East")
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{output_database}")

    print(f"Saving transformed table to: {output_table_full_name}")
    transform_df.write.format("delta").mode("overwrite").saveAsTable(
        output_table_full_name
    )
    
    return transform_df
