
# Get Club Stats for the Season for a Team

#     Endpoint: /v1/club-stats-season/{team}
#     Method: GET
#     Description: Returns an overview of the stats for each season for a specific club. Seems to only indicate the gametypes played in each season.
#     Parameters:
#         team (string) - Three-letter team code
#     Response: JSON format

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

from app.extraction.generic_get_results import make_request, save_json

DIR = "data/api_data"

#URL = 'https://api-web.nhle.com/v1/club-stats-season/'

def team_list(file_name)-> list:
    ls = pd.read_csv(file_name)
    ls = ls['triCode'].tolist()
    return ls 

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    
    for team in team_list('data/csv_data/raw/raw_teams.csv'):
        URL = f'https://api-web.nhle.com/v1/club-stats-season/{team}'
        fetch_data(URL,f"team_season_{team}", DIR)
