import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import shutil
import glob
import pandas as pd
import json
import time
from loguru import logger
from datetime import date
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk


LOG_FILE = "app/raw_all_goalies_stats_ETL/raw_all_goalie__stats_log_log.log"
logger.remove()
logger.add(
    sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO"
)
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB",
)


def extract_goalie_stats():
    """
    #### Daily Update
    Goalie stats per season, update daily
    """
    URL = "https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&cayenneExp=seasonId={season_id}"
    OUTPUT_DIR = "data/json_data/raw_all_goalies_stats/landing"
    PARAMETER_FILE = "app/api_parameters/season_ids.csv"

    df_parameter = pd.read_csv(PARAMETER_FILE)

    ## TO GET RECENT
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    try:
        data, _ = make_request(url)
        save_json(f"raw_stats_all_goalies_{max_season_id}", data, OUTPUT_DIR)
        logger.info(f"Data collected --- {url}")
    except Exception as e:
        logger.error(f"Failed to collect: {url}---{e}")


def transform_goalie_stats():
    PATTERN = (
        "data/json_data/raw_all_goalies_stats/landing/raw_stats_all_goalies_*.json"
    )
    OUTPUT_DIR = "data/csv_data/raw/raw_all_goalies_stats/staging"
    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)
            with open(input_file) as f:
                data = json.load(f)

            try:
                ## NORMALIZADO EM UMA LINHA
                df = pd.json_normalize(data[0], record_path=["data"], errors="ignore")
            except Exception:
                df = pd.json_normalize(data, record_path=["data"], errors="ignore")

            df["filename"] = file

            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
        except FileNotFoundError as e:
            logger.error(f"Error --- {e}")


def append_goalies_stats():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "all_goalies_stats"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_all_goalies_stats/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_goalies_stats(file):
    try:
        update_table_with_sk(
            file, "raw_all_goalies_stats", ["playerid", "seasonid", "gamesplayed"]
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_all_goalies_stats")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_all_goalies_stats/landing"
    JSON_DIR = "data/json_data/raw_all_goalies_stats"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_all_goalies_stats/staging"
    CSV_DIR = "data/csv_data/raw/raw_all_goalies_stats"
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

    extract_goalie_stats()
    transform_goalie_stats()
    appended_file = append_goalies_stats()
    loaded_file = load_goalies_stats(appended_file)
    clear_staging_landing_loading(loaded_file)

    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes = int(elapsed_time // 60)
    seconds = elapsed_time % 60
    logger.info(
        f"✅✅✅ Pipeline completed succesfuly in {minutes}min. {seconds:.0f}s."
    )


if __name__ == "__main__":
    # extract_goalie_stats()
    # transform_goalie_stats()
    # append_goalies_stats()
    # load_goalies_stats()
    run()
    pass
