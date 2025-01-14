# UPDATE FREQUENCY: DAILY

# Skaters
# Get Current Skater Stats Leaders

#     Endpoint: /v1/skater-stats-leaders/current
#     Method: GET
#     Description: Retrieve current skater stats leaders.
#     Parameters:
#         categories (query, string) - Optional
#         limit (query, int) - Optional (Note: a limit of -1 will return all results)
#     Response: JSON format


import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api-web.nhle.com/v1/skater-stats-leaders/20242025/2?categories=goals&limit=-1'

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_data(URL,"stats_current_goals_per_player", DIR)

