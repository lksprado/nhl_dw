## RETORNA LISTA DOS JOGADORES POR TIME
#https://github.com/Zmalski/NHL-API-Reference?tab=readme-ov-file#get-team-roster-by-season
##Get Team Roster by Season
    # Endpoint: /v1/roster/{team}/{season}
    # Method: GET
    # Description: Retrieve the roster for a specific team and season.
    # Parameters:
    #     team (string) - Three-letter team code
    #     season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
    # Response: JSON format
    
# IT WILL BE NECESSARY TO ITERATE TEAM AND SEASONS TO GET ALL PLAYER ID'S
# https://api-web.nhle.com/v1/roster/EDM/20242025

import os
import sys
import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.extraction.generic_get_results import make_request, save_json

DIR = "data/json_data/raw_roster_season"

# URL = 'https://api-web.nhle.com/v1/roster/TOR/20232024'
URL = 'https://api-web.nhle.com/v1/roster/{team_id}/{season_id}'

def team_list(file_name)-> list:
    df = pd.read_csv(file_name)
    urls = [URL.format(team_id=row['team_id'], season_id=row['season_id']) for index, row in df.iterrows()]
    return urls 

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    urls = team_list('data/csv_data/processed/parameters_team_season.csv')
    for url in urls:
        team_id = url.split('/')[-2]
        season_id = url.split('/')[-1]
        file_name = f"roster_{team_id}_{season_id}"
        fetch_data(url, file_name, DIR)
        print(f"Data fetched for {team_id} in season {season_id}")
