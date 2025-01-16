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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api-web.nhle.com/v1/roster/TOR/20232024'

def fetch_data(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_data(URL,"roster_per_season", DIR)


