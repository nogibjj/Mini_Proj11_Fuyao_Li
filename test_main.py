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
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
url = f"https://{server_h}/api/2.0"


# Validate environment variables
if not server_h or not access_token:
    raise ValueError("SERVER_HOSTNAME and ACCESS_TOKEN must be set in the .env file.")


def check_filestore_path(path, headers): 
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False


def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(FILESTORE_PATH, headers) is True


if __name__ == "__main__":
    test_databricks()
