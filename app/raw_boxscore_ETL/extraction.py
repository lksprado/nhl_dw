import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## raw_boxscore_games/raw_boxscore_players
def fetch_and_save_boxscore(game_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"{game_id}_boxcore", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


## raw_boxscore_games/raw_boxscore_players
def get_game_boxcore():
    """
    #### Daily Update
    ---
    """
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    OUTPUT_DIR = "data/json_data/raw_boxscore"

    parameters_input = "app/all_games_till_2025-01-29.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(by=["game_id"], ascending=False)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for row in df_parameter.itertuples():
            game_ids = row.game_id
            url = URL.format(game_id=game_ids)
            futures.append(
                executor.submit(
                    fetch_and_save_boxscore,
                    game_ids,
                    url,
                    OUTPUT_DIR,
                )
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
