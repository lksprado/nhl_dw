# UPDATE: YEARLY

# Get Seasons

#     Endpoint: /v1/season
#     Method: GET
#     Description: Retrieve a list of all season IDs past & present in the NHL.
#     Response: JSON format
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api-web.nhle.com/v1/season'

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_data(URL,"seasons", DIR)

