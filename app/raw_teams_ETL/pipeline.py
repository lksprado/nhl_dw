import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import json
import pandas as pd
from app.extraction.generic_get_results import make_request, save_json
from app.loading.data_loader_duckdb import truncate_and_load_table
from utils.time_tracker import track_time


@track_time
def extract_teams():
    """
    #### Yearly update \n
    Returns teams basic information
    """
    URL = "https://api.nhle.com/stats/rest/en/team"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("raw_teams", data, OUTPUT_DIR)


@track_time
def transform_teams():
    INPUT_FILE = "data/json_data/single/raw_teams.json"
    OUTPUT_PATH = "data/csv_data/raw/single"

    file = os.path.basename(INPUT_FILE)
    file = os.path.splitext(file)[0]

    file_path = os.path.join(OUTPUT_PATH, file)

    with open(INPUT_FILE) as f:
        data = json.load(f)
        df = pd.json_normalize(data["data"])

    df.to_csv(file_path + ".csv", index=False)


@track_time
def load_teams():
    truncate_and_load_table("data/csv_data/raw/single/raw_teams.csv", "raw_teams")


if __name__ == "__main__":
    extract_teams()
    transform_teams()
    load_teams()
    pass
