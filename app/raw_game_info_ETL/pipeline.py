import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import time
import json
import pandas as pd
from loguru import logger
from app.extraction.generic_get_results import make_request, save_json
from app.loading.data_loader_duckdb import update_table_with_pk


LOG_FILE = "app/raw_game_info_ETL/raw_gameinfo_log_log.log"
logger.remove()
logger.add(
    sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO"
)
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="2 MB",
)


def extract_game_info():
    """
    #### Daily Update
    Returns all time game id's schedules and game results
    """
    URL = "https://api.nhle.com/stats/rest/en/game"
    OUTPUT_DIR = "data/json_data/single"

    try:
        data, _ = make_request(URL)
        save_json("raw_game_info", data, OUTPUT_DIR)
        logger.info(f"Data collected: {URL}")
    except Exception as e:
        logger.error(f"Failed to collect {URL} --- {e}")


def transform_game_info():
    INPUT_FILE = "data/json_data/single/raw_game_info.json"
    OUTPUT_DIR = "data/csv_data/processed/flow_loads"

    try:
        file = os.path.basename(INPUT_FILE)
        file = os.path.splitext(file)[0]
        file_path = os.path.join(OUTPUT_DIR, file)

        with open(INPUT_FILE) as f:
            data = json.load(f)
            df = pd.json_normalize(data["data"])

        output_file = file_path + ".csv"
        df.to_csv(output_file, index=False)
        logger.info(f"File saved: {output_file}")
        return output_file
    except FileNotFoundError as e:
        logger.error(f"Error --- {e}")


def load_game_info(file):
    try:
        update_table_with_pk(file, "raw_game_info", "id")
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_game_details")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def run():
    start_time = time.time()

    try:
        extract_game_info()
        load_file = transform_game_info()

        if load_file:
            load_game_info(load_file)

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.info(
            f"✅✅✅ Pipeline completed successfully in {minutes}min {seconds:.0f}s."
        )

    except Exception as e:
        logger.error(f"❌❌❌ Pipeline failed --- {e}")


if __name__ == "__main__":
    run()
    pass
