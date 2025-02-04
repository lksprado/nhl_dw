import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## raw_player_info
def fetch_and_save_player_info(player_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"player_{player_id}_info", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


## raw_player_info
def get_player_info():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/player/{player}/game-log/{season}/{game-type}
    Method: GET
    Description: Retrieve the game log for a specific player, season, and game type.
    Parameters:
        player (int) - Player ID
        season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
        game-type (int) - Game type (guessing 2 for regular season, 3 for playoffs)
    Response: JSON format
    """
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/player/{player_id}/landing"

    OUTPUT_DIR = "data/json_data/raw_player_info"

    parameters_input = "data/csv_data/processed/parameters_players.csv"
    df_parameter = pd.read_csv(parameters_input)

    pattern = os.path.join(OUTPUT_DIR, "player_*_info.json")
    files_to_skip = glob.glob(pattern)
    existing_combinations = set()
    for file in files_to_skip:
        match = re.search(r"player_(\d+)_info\.json", os.path.basename(file))
        if match:
            player_id = match.group(1)
            existing_combinations.add((player_id))

    df_filtered = df_parameter[
        ~df_parameter.apply(
            lambda row: str(row["player_id"]) in existing_combinations, axis=1
        )
    ]

    df_filtered = df_filtered["player_id"].unique()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for player_id in df_filtered:
            url = URL.format(player_id=player_id)
            futures.append(
                executor.submit(fetch_and_save_player_info, player_id, url, OUTPUT_DIR)
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")

        end_time = time.time()
        dif = end_time - start_time
        hours, remainder = divmod(dif, 3600)
        minutes, seconds = divmod(remainder, 60)

        print(f"Done in {int(hours)}h {int(minutes)}m  {int(seconds)}s")
