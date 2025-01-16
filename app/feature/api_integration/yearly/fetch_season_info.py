# UPDATE FREQUENCY: YEARLY
# DISMISS LATEST IF ON REGULAR SEASON

# Get Season

#     Endpoint: /{lang}/season
#     Method: GET
#     Description: Retrieve season information.
#     Response: JSON format

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api.nhle.com/stats/rest/en/season'

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_data(URL,"season_info", DIR)






