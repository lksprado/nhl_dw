import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## raw_roster_season
def get_team_roster_by_season():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/roster/{team}/{season}
    Method: GET
    Description: Retrieve the roster for a specific team and season.
    Parameters:
        team (string) - Three-letter team code
        season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
    Response: JSON format
    """

    # BUILD URLS
    URL = "https://api-web.nhle.com/v1/roster/{team_id}/{season_id}"
    OUTPUT_DIR = "data/json_data/raw_roster_season"

    parameters_input = "data/csv_data/processed/parameters_team_season.csv"
    df_parameter = pd.read_csv(parameters_input)

    urls = [
        URL.format(team_id=row.team_id, season_id=row.season_id)
        for row in df_parameter.itertuples()
    ]

    # LOOP THROUGH URLS
    for url in urls:
        team_id = url.split("/")[-2]
        season_id = url.split("/")[-1]
        file_name = f"roster_{team_id}_{season_id}"

        data, _ = make_request(url)
        save_json(file_name, data, OUTPUT_DIR)
