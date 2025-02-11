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
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk


LOG_FILE = "app/raw_all_game_log_ETL/raw_all_game_log_log.log"
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


## raw_all_game_log
def fetch_and_save_game_log(player_id, season_id, season_step, url, output_dir):
    """Abstract function for multi-threading use"""
    data, _ = make_request(url)
    logger.info(f"Data Collected --- {url}")
    save_json(f"{player_id}_{season_id}_{season_step}", data, output_dir)


def extract_game_log():
    """
    #### Daily Update
    Makes the API call based on parameters and saves json file
    """

    URL = "https://api-web.nhle.com/v1/player/{player_id}/game-log/{season_id}/{season_type}"
    OUTPUT_DIR = "data/json_data/raw_game_log/landing"

    df_players = pd.read_csv("app/api_parameters/active_playerid.csv")
    df_season_game = pd.read_csv("app/api_parameters/seasonid_teamid_game_type.csv")
    df_season_game = df_season_game[["season_id", "game_type"]].drop_duplicates()

    max_season_id = df_season_game["season_id"].max()
    max_game_type = df_season_game["game_type"].max()
    df_filtered = df_season_game[df_season_game["season_id"] == max_season_id]
    max_game_type = df_filtered["game_type"].max()
    df_team_season = df_filtered[df_filtered["game_type"] == max_game_type]
    df_filtered = df_players.merge(df_team_season, how="cross")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in df_filtered.itertuples():
            player_id = row.playerid
            season_id = row.season_id
            game_type_id = row.game_type
            url = URL.format(
                player_id=player_id, season_id=season_id, season_type=game_type_id
            )
            futures.append(
                executor.submit(
                    fetch_and_save_game_log,
                    player_id,
                    season_id,
                    game_type_id,
                    url,
                    OUTPUT_DIR,
                )
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error --- {e}")


def process_single_file(input_file, output_dir):
    """
    Process a single Json and save it as CSV
    """
    try:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join(output_dir, file)
        with open(input_file) as f:
            data = json.load(f)

        ## NORMALIZADO EM UMA LINHA
        df = pd.json_normalize(
            data,
            record_path=["gameLog"],
            meta=["seasonId", "gameTypeId"],
            errors="ignore",
        )
        df["filename"] = file

        output_file = file_path + ".csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Saved file: {output_file}")
    except Exception as e:
        logger.error(f"Error in file {input_file} --- {e}")


def transform_game_log():
    """Transform all json to csv files"""
    PATTERN = "data/json_data/raw_game_log/landing/*_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_game_log/staging"
    input_files = glob.glob(PATTERN)

    with Pool(cpu_count()) as pool:
        pool.starmap(process_single_file, [(file, OUTPUT_DIR) for file in input_files])


def append_game_log():
    """Concatenates all csv files to a single one for loading purposes"""
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "all_game_log"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_game_log/staging"
    OUTPUT_CSV_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_CSV_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_CSV_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_game_log(file):
    """Loads csv file through upserting"""
    try:
        update_table_with_sk(
            file,
            "raw_game_log",
            ["gameid", "filename"],
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_game_log")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    """Move files for storage"""
    JSON_LANDING_DIR = "data/json_data/raw_game_log/landing"
    JSON_DIR = "data/json_data/raw_game_log"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_game_log/staging"
    CSV_DIR = "data/csv_data/raw/raw_game_log"
    LOAD_DIR = "data/csv_data/processed"
    STORED_LOADS = (
        "/media/lucas/Files/2.Projetos/nhl-dw/data/csv_data/processed/flow_loads"
    )

    ## MOVE JSON FILES
    try:
        for filename in os.listdir(JSON_LANDING_DIR):
            source_path = os.path.join(JSON_LANDING_DIR, filename)
            destination_path = os.path.join(JSON_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Json files in {JSON_LANDING_DIR} moved to {JSON_DIR}")
    except FileNotFoundError as e:
        logger.warning(f"Failed to move json files --- {e}")

    ## MOVE CSV FILES
    try:
        for filename in os.listdir(CSV_STAGING_DIR):
            source_path = os.path.join(CSV_STAGING_DIR, filename)
            destination_path = os.path.join(CSV_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Csv files in {CSV_STAGING_DIR} moved to {CSV_DIR}")
    except FileNotFoundError:
        logger.warning("Failed to move csv files")

    ## MOVE LOAD FILE
    try:
        file_only = os.path.basename(file)
        source_path = os.path.join(LOAD_DIR, file_only)
        destination_path = os.path.join(STORED_LOADS, file_only)
        shutil.move(source_path, destination_path)
    except FileNotFoundError as e:
        logger.warning(f"Failed to move loaded csv file -- {e}")


def run():
    try:
        start_time = time.time()

        extract_game_log()
        transform_game_log()
        appended_file = append_game_log()
        loaded_file = load_game_log(appended_file)
        clear_staging_landing_loading(loaded_file)

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.info(
            f"✅✅✅ Pipeline completed succesfuly in {minutes}min. {seconds:.0f}s."
        )
    except Exception as e:
        logger.error(f"❌❌❌ Pipeline failed --- {e}")


if __name__ == "__main__":
    run()
    pass
