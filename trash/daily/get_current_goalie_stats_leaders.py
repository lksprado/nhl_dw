# UPDATE FREQUENCY: DAILY

# Get Current Goalie Stats Leaders

#     Endpoint: /v1/goalie-stats-leaders/current
#     Method: GET
#     Description: Retrieve current goalie stats leaders.
#     Request Parameters:
#         categories (query, string) - Optional
#         limit (query, int) - Optional (Note: a limit of -1 will return all results)
#     Response: JSON format

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.extraction.generic_get_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api-web.nhle.com/v1/goalie-stats-leaders/current?categories=wins&limit=-1'

def fetch_data(url, file_name: str, output_json_dir):
    data = make_request(url)
    save_json(file_name, data, output_json_dir)

fetch_data(URL,"stats_current_goalies", DIR)



