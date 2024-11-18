"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests


def extract(
        url1="https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true", 
        file_path1="dbfs:/FileStore/mini_project11/trump_mini11.csv",
    ):
    """Extract urls to a file path"""
    local_path = "/tmp/trump.csv"
    response1 = requests.get(url1)
    if response1.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response1.content)
        print(f"File successfully downloaded to {local_path}")
    else:
        print(
            f"Failed to retrieve the file from {url1}."
        )

    dbutils.fs.cp(f"file://{local_path}", file_path1)

    print(f"CSV successfully saved to {file_path1}")
    return file_path1


if __name__ == "__main__":
    extract()