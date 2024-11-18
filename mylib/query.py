from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Query").getOrCreate()


def query(
    catalog="ids706_data_engineering",
    input_database="fuyao_db",
    output_database="fuyao_db_processed",
    output_table_name="rally_larger_than_40",
):
    input_table_full_name = f"{catalog}.{input_database}.rally"
    query = f"SELECT * FROM {input_table_full_name} WHERE lat > 40"
    output_table_full_name = f"{catalog}.{output_database}.{output_table_name}"

    result_df = spark.sql(query)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{output_database}")

    result_df.write.format("delta").mode("overwrite").saveAsTable(
        output_table_full_name
    )

    print(f"Query result successfully saved to table: {output_table_full_name}")


if __name__ == "__main__":
    query()