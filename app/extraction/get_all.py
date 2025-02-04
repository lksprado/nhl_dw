import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## SERVE PARA CRIAR PARAMETROS CADA TEMPORADA E QUAL TIPO DE JOGO FOI JOGADO
def get_club_stats_for_the_season_for_a_team():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/club-stats-season/{team}
    Method: GET
    Description: Returns an overview of the stats for each season for a specific club. Seems to only indicate the gametypes played in each season.
    Parameters:
        team (string) - Three-letter team code
        season_type (int) - Type of the season (default is 2)
    Response: JSON format
    """
    INPUT_FILE = "data/csv_data/raw/single/raw_teams.csv"
    OUTPUT_DIR = "data/json_data/raw_team_season"

    ls = pd.read_csv(INPUT_FILE)
    ls = ls["triCode"]
    for team in ls:
        URL = f"https://api-web.nhle.com/v1/club-stats-season/{team}"
        OUTPUT_DIR = "data/json_data/raw_team_season"

        data, _ = make_request(URL)
        save_json(f"team_season_{team}", data, OUTPUT_DIR)


## NAO IMPLEMENTADO
# def get_club_stats_now(season_type=2):
#     """
#     #### Daily Update
#     ---
#     #### API Endpoint Documentation
#     Endpoint: /v1/club-stats/{team}/now
#     Method: GET
#     Description: Retrieve current statistics for a specific club.
#     Parameters:
#         team (string) - Three-letter team code
#         season_type (int) - Type of the season (default is 2)
#     Response: JSON format
#     """

#     URL = "https://api-web.nhle.com/v1/club-stats/{team_id}/{season_id}/{season_step}"
#     OUTPUT_DIR = "data/json_data/raw_club_stats"

#     parameters_input = "data/csv_data/processed/parameters_team_season.csv"
#     df_parameter = pd.read_csv(parameters_input)
#     max_season_id = df_parameter["season_id"].max()
#     df_filtered = df_parameter[df_parameter["season_id"] == max_season_id]
#     unique_teams = df_filtered["team_id"].unique()

#     for team_id in unique_teams:
#         url = URL.format(
#             team_id=team_id, season_id=max_season_id, season_step=season_type
#         )
#         data, _ = make_request(url)
#         save_json(
#             f"stats_club_{team_id}_{max_season_id}_{season_type}", data, OUTPUT_DIR
#         )

# TO GET HISTORIC DATA
# urls = [URL.format(team_id=row.team_id, season_id=row.season_id, season_step = season_type) for row in df_parameter.itertuples()]
# for url in urls:
#     team_id = url.split('/')[-3]
#     season_id = url.split('/')[-2]
#     file_name = f"stats_club_now_{team_id}_{season_id}_{season_step}"
#     data, _ = make_request(url)
#     save_json(file_name, data, OUTPUT_DIR)
#     print(f"Data fetched for {team_id} in season {season_id}")
