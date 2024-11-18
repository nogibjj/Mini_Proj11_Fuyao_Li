"""
Test goes here

"""
import requests
from dotenv import load_dotenv
import os


# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOST_NAME")
access_token = os.getenv("DATABRICKS_KEY")
# FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
url = f"https://{server_h}/api/2.0/dbfs/get-status"


def check_filestore_path(headers): 
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False


def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(headers) is True


if __name__ == "__main__":
    test_databricks()
