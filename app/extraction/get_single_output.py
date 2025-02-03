import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from app.extraction.generic_get_results import make_request, save_json

#### GERAM SOMENTE UM ARQUIVO NA PASTA /media/lucas/Files/2.Projetos/nhl-dw/data/json_data/single


#### VERY IMPORTANT - BASE FOR ALL GAME IDS
## RAW_GAME_INFO
def get_game_info():
    """
    Returns all time game id's. Update per each regular season and post season begining
    """
    URL = "https://api.nhle.com/stats/rest/en/game"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("raw_game_info", data, OUTPUT_DIR)


#### OPCIONAL SE IMPLENTAR A SEASON_INFO QUE É MAIS COMPLETA
def get_season_ids():
    """
    Returns a list of all seasons
    """
    URL = "https://api-web.nhle.com/v1/season"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("season_ids", data, OUTPUT_DIR)


#### NAO MODELADA - MAS PODE SER ÚTIL PARA CRIAR DIMENSÃO DE SEASONS
def get_season_info():
    """
    Returns all seasons details like start/end dates, number of regular and post season games
    """
    URL = "https://api.nhle.com/stats/rest/en/season"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("season_info", data, OUTPUT_DIR)


### NAO MODELADA
def get_standings_now():
    """
    Daily Update
    Returns current stats for each team
    """
    URL = "https://api-web.nhle.com/v1/standings/now"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("standings_now", data, OUTPUT_DIR)


# RAW_TEAMS
def get_teams():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/team
    Method: GET
    Description: Retrieve list of all teams.
    Parameters:
        lang (string) - Language code
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/team"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("teams", data, OUTPUT_DIR)
