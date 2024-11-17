"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests
import os
from dotenv import load_dotenv
import dbutils
import pandas as pd


# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOST_NAME")
access_token = os.getenv("DATABRICKS_KEY")
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"


def extract(
        url1="https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true", 
        file_path1=f"{FILESTORE_PATH}/fuyao_databricks/data/trump.csv",
    ):
    """Extract urls to a file path"""
    local_path = "/tmp/trump.csv"
    response1 = requests.get(url1)
    if response1.status_code == 200:
        df = pd.read_csv(url1)
        df.to_csv(local_path, index=False)
        print(f"File successfully downloaded to {local_path}")
    else:
        print(
            f"Failed to retrieve the file from {url1}."
        )

    dbutils.fs.mkdirs(FILESTORE_PATH)
    dbutils.fs.cp(f"file:{local_path}", file_path1)
    print(f"CSV successfully saved to {file_path1}")
    return file_path1


if __name__ == "__main__":
    extract()