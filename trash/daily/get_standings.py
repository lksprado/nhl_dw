# UPDATE FREQUENCY: DAILY

# Standings
# Get Standings

#     Endpoint: /v1/standings/now
#     Method: GET
#     Description: Retrieve the standings as of the current moment.
#     Response: JSON format

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api-web.nhle.com/v1/standings/now'

def fetch_seasons(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_seasons(URL,"current_standings", DIR)



