import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import shutil
import json
import time
import glob
from loguru import logger
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_pk

LOG_FILE = "app/raw_player_info_ETL/raw_playerinfo_ETL_log_log.log"
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


def fetch_and_save_player_info(player_id, url, output_dir):
    data, _ = make_request(url)
    save_json(f"player_{player_id}_info", data, output_dir)
    logger.info(f"Data collected --- {url}")


def extract_player_info():
    """
    #### Daily Update
    """
    URL = "https://api-web.nhle.com/v1/player/{player_id}/landing"
    OUTPUT_DIR = "data/json_data/raw_player_info/landing"

    df = pd.read_csv("app/api_parameters/active_playerid.csv")

    df = df["playerid"]

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = []
        for player_id in df:
            url = URL.format(player_id=player_id)
            futures.append(
                executor.submit(fetch_and_save_player_info, player_id, url, OUTPUT_DIR)
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Exception occurred: {e}")


def transform_player_info():
    PATTERN = "data/json_data/raw_player_info/landing/player_*_info.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_player_info/staging"

    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
                data = json.load(f)
                df = pd.json_normalize(data)

            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
        except Exception as e:
            logger.error(f"Error --- {e}")


def append_player_info():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "player_info"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_player_info/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file
    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_player_info(file):
    try:
        update_table_with_pk(file, "raw_player_info", "playerid")
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_player_info")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_player_info/landing"
    JSON_DIR = "data/json_data/raw_player_info"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_player_info/staging"
    CSV_DIR = "data/csv_data/raw/raw_player_info"
    LOAD_DIR = "data/csv_data/processed"
    STORED_LOADS = "data/csv_data/processed/flow_loads"

    ## MOVE JSON FILES
    try:
        for filename in os.listdir(JSON_LANDING_DIR):
            source_path = os.path.join(JSON_LANDING_DIR, filename)
            destination_path = os.path.join(JSON_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Json files in {JSON_LANDING_DIR} moved to {JSON_DIR}")
    except Exception as e:
        logger.warning(f"Failed to move json files --- {e}")

    ## MOVE CSV FILES
    try:
        for filename in os.listdir(CSV_STAGING_DIR):
            source_path = os.path.join(CSV_STAGING_DIR, filename)
            destination_path = os.path.join(CSV_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Csv files in {CSV_STAGING_DIR} moved to {CSV_DIR}")
    except Exception as e:
        logger.warning(f"Failed to move csv files {e}")

    ## MOVE LOAD FILE
    try:
        file_only = os.path.basename(file)
        source_path = os.path.join(LOAD_DIR, file_only)
        destination_path = os.path.join(STORED_LOADS, file_only)
        shutil.move(source_path, destination_path)
    except Exception as e:
        logger.warning(f"Failed to move loaded csv file -- {e}")


def run():
    start_time = time.time()

    try:
        extract_player_info()
        transform_player_info()
        appended_file = append_player_info()

        if appended_file:
            loaded_file = load_player_info(appended_file)

            if loaded_file:
                clear_staging_landing_loading(loaded_file)

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
