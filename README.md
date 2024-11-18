[![CI](https://github.com/nogibjj/Mini_Proj10_Fuyao_Li/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Mini_Proj10_Fuyao_Li/actions/workflows/cicd.yml)
# Mini Project 11

### Fuyao Li

## Requirements
+ Use PySpark to perform data processing on a large dataset
+ Include at least one Spark SQL query and one data transformation

## Project Structure
The ETL pipeline in this project is divided into three main stages:

+ Extract: Retrieve data from a remote source or a local file.
+ Transform: Process and manipulate the data, such as filtering, aggregating, or creating new columns to derive meaningful insights.
+ Load: Store the processed data into a PySpark DataFrame, making it ready for analysis or downstream operations.

## Key Features
+ Data Extraction: Supports data retrieval from both remote sources and local CSV files.
+ Data Transformation: Performs a variety of transformations including data filtering, aggregation, and calculation of new columns.
+ Data Loading: Loads the transformed data into a PySpark DataFrame for further processing and analysis.
+ Testing: Comprehensive unit tests to ensure the robustness of the ETL pipeline components using pytest and PySpark.

## Installation
+ Clone the repository:
``` shell
git@github.com:nogibjj/Mini_Proj10_Fuyao_Li.git
```
+ Set up environment:
``` shell
pip install -r requirements.txt
```

## Project Structure
```plaintext
Mini Proj10/
│
├── mylib/                    
│   ├── calculator.py          
├── data/                     
│   └── trump.csv
|
├── README.md                
├── requirements.txt          
|── main.py                   
└── test_main.py
```

## Explanation
1. Spark Session Creation: Initializes and returns a PySpark session, enabling the Spark environment for subsequent data processing tasks.
```python
def create_spark_session(appName):
    """Create and return a Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    log_output("Spark session created")
    return spark
```
2. Data Extraction: Fetches data from a specified URL and saves it to a local file path.
```python
def extract(url1, file_path1):
    """Extract URLs to a file path."""
    os.makedirs(os.path.dirname(file_path1), exist_ok=True)
    response1 = requests.get(url1)
    if response1.status_code == 200:
        with open(file_path1, "wb") as f:
            f.write(response1.content)
        print(f"File successfully downloaded to {file_path1}")
    else:
        print(f"Failed to retrieve the file from {url1}.")
    return file_path1
```
3. Data Loading: Reads the downloaded CSV file into a PySpark DataFrame. 
```python
def load(spark, data="data/trump.csv"):
    """Load data with schema defined for preprocessing."""
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
```
4. Data Querying: Creates a temporary SQL view of the DataFrame and performs a query to filter rows where the latitude (lat) is greater than 40.
```python
def query_data(spark, df):
    df.createOrReplaceTempView("trump")
    result = spark.sql("SELECT * FROM trump WHERE lat > 40")
    log_output("Query data", result.limit(10).toPandas().to_markdown())
    return result
```
5. Data Transformation: Adds a Region column to the DataFrame based on longitude (lng). The when function from PySpark is used to categorize rows into "West," "Central," or "East" regions. The transformed data is logged for review.
```python
def transform(df):
    """Apply transformations to the DataFrame."""
    transform_df = df.withColumn(
        "Region",
        when(col("lng") < -100, "West")
        .when((col("lng") < -80) & (col("lng") >= -100), "Central")
        .when(col("lng") >= -80, "East")
    )
    log_output("transform data", transform_df.limit(10).toPandas().to_markdown())
    return transform_df
```


## Logged Output
The logged markdown file could be found in `pyspark_log.md`.

## References
https://github.com/nogibjj/python-ruff-template

## Data resource:
https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv
