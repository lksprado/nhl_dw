import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from app.extraction.generic_get_results import make_request, save_json


# RAW_TEAMS
def get_teams():
    """
    #### Yearly update \n
    Returns teams basic information
    """
    URL = "https://api.nhle.com/stats/rest/en/team"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("teams", data, OUTPUT_DIR)
