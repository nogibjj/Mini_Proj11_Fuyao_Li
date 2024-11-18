[![CI](https://github.com/nogibjj/Mini_Proj10_Fuyao_Li/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Mini_Proj10_Fuyao_Li/actions/workflows/cicd.yml)
# Mini Project 11

### Fuyao Li

## Requirements
+ Create a data pipeline using Databricks
+ Include at least one data source and one data sink

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
git@github.com:nogibjj/Mini_Proj11_Fuyao_Li.git
```
+ Set up environment:
``` shell
pip install -r requirements.txt
```

## Project Structure
```plaintext
Mini Proj11/
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


## References
https://github.com/nogibjj/python-ruff-template

## Data resource:
https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv
