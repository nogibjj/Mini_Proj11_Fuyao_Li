from mylib.calculator import (
    log_output,
    create_spark_session,
    extract,
    load,
    query_data,
    transform
)

if __name__ == "__main__":
    # Main ETL process
    spark = create_spark_session("PySparkTrump")

    # Extract
    extract()

    # Load
    df = load(spark)

    # Query
    query_data(spark, df)

    # Transform
    transform(df)

    # Stop the Spark session
    spark.stop()
    log_output("Spark session stopped.")
