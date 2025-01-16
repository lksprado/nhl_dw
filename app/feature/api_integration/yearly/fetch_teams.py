# UPDATE YEARLY

# Get Team Information

#     Endpoint: /{lang}/team
#     Method: GET
#     Description: Retrieve list of all teams.
#     Parameters:
#         lang (string) - Language code
#     Response: JSON format

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api.nhle.com/stats/rest/en/team'

def fetch_seasons(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_seasons(URL,"teams", DIR)




