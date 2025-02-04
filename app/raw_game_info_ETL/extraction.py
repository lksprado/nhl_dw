import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from app.extraction.generic_get_results import make_request, save_json


#### VERY IMPORTANT - BASE FOR ALL GAME IDS
## RAW_GAME_INFO
def get_game_info():
    """
    #### Daily Update
    Returns all time game id's. Update per each regular season and post season begining
    """
    URL = "https://api.nhle.com/stats/rest/en/game"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("raw_game_info", data, OUTPUT_DIR)
