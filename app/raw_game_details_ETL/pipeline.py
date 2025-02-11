import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import glob
import pandas as pd
import json
import time
from loguru import logger
from datetime import date
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_pk

LOG_FILE = "app/raw_game_details_ETL/raw_gamedetails_ETL_log_log.log"
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


def fetch_and_save_game_details(game_id, url, output_dir):
    data, _ = make_request(url)
    logger.info(f"Data collected --- {url}")
    save_json(f"{game_id}_details", data, output_dir)


def extract_game_details():
    URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail"
    OUTPUT_DIR = "data/json_data/raw_game_details/landing"

    df = pd.read_csv("app/api_parameters/gamedetails_gameids.csv")

    games = df["id"]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for game_id in games:
            url = URL.format(game_id=game_id)
            futures.append(
                executor.submit(
                    fetch_and_save_game_details,
                    game_id,
                    url,
                    OUTPUT_DIR,
                )
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error --- {e}")


def transform_game_details():
    PATTERN = "data/json_data/raw_game_details/landing/*_details.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_game_details/staging"

    input_files = glob.glob(PATTERN)

    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
                data = json.load(f)

            ## NORMALIZADO EM UMA LINHA
            df_refs = pd.json_normalize(
                data,
                record_path=["gameInfo", "referees"],
                record_prefix="gameInfo.referees.",
                errors="ignore",
            )
            df_refs["filename"] = file
            df_refs["referee_index"] = df_refs.groupby("filename").cumcount() + 1

            df_refs = df_refs.pivot(
                index="filename",
                columns="referee_index",
                values="gameInfo.referees.default",
            )
            df_refs.columns = [f"referee_{col}" for col in df_refs.columns]

            # NORMALIZADO EM UMA LINHA
            df_final_score = pd.json_normalize(data)
            df_final_score = df_final_score[
                ["linescore.totals.away", "linescore.totals.home"]
            ]
            df_final_score = df_final_score.rename(
                columns={
                    "linescore.totals.away": "final_score_away",
                    "linescore.totals.home": "final_score_home",
                }
            )
            df_final_score["filename"] = file

            ## NORMALIZADO EM UMA LINHA
            df_period_score = pd.json_normalize(
                data,
                record_path=["linescore", "byPeriod"],
                record_prefix="linescore.byPeriod.",
                errors="ignore",
            )
            df_home = df_period_score[
                [
                    "linescore.byPeriod.home",
                    "linescore.byPeriod.periodDescriptor.number",
                ]
            ]
            df_home = df_home.rename(
                columns={
                    "linescore.byPeriod.home": "score_home",
                    "linescore.byPeriod.periodDescriptor.number": "period",
                }
            )
            df_home["filename"] = file
            df_away = df_period_score[
                [
                    "linescore.byPeriod.away",
                    "linescore.byPeriod.periodDescriptor.number",
                ]
            ]
            df_away = df_away.rename(
                columns={
                    "linescore.byPeriod.away": "score_away",
                    "linescore.byPeriod.periodDescriptor.number": "period",
                }
            )
            df_away["filename"] = file
            df_home = df_home.pivot(
                index="filename", columns="period", values="score_home"
            )
            df_away = df_away.pivot(
                index="filename", columns="period", values="score_away"
            )
            df_home.columns = [f"score_home_period_{col}" for col in df_home.columns]
            df_away.columns = [f"score_away_period_{col}" for col in df_away.columns]
            df_period_score = pd.concat([df_home, df_away], axis=1).reset_index()

            # NORMALIZADO EM UMA LINHA
            df_period_shots = pd.json_normalize(
                data,
                record_path=["shotsByPeriod"],
                record_prefix="shotsByPeriod.",
                errors="ignore",
            )
            df_home_shots = df_period_shots[
                ["shotsByPeriod.home", "shotsByPeriod.periodDescriptor.number"]
            ]
            df_home_shots = df_home_shots.rename(
                columns={
                    "shotsByPeriod.home": "shots_home",
                    "shotsByPeriod.periodDescriptor.number": "period",
                }
            )
            df_away_shots = df_period_shots[
                ["shotsByPeriod.away", "shotsByPeriod.periodDescriptor.number"]
            ]
            df_away_shots = df_away_shots.rename(
                columns={
                    "shotsByPeriod.away": "shots_away",
                    "shotsByPeriod.periodDescriptor.number": "period",
                }
            )
            df_home_shots["filename"] = file
            df_home_shots = df_home_shots.pivot(
                index="filename", columns="period", values="shots_home"
            )
            df_away_shots["filename"] = file
            df_away_shots = df_away_shots.pivot(
                index="filename", columns="period", values="shots_away"
            )
            df_home_shots.columns = [
                f"shots_home_period_{col}" for col in df_home_shots.columns
            ]
            df_away_shots.columns = [
                f"shots_away_period_{col}" for col in df_away_shots.columns
            ]
            df_period_shots = pd.concat(
                [df_home_shots, df_away_shots], axis=1
            ).reset_index()
            df_period_shots["filename"] = file

            # NORMALIZADO EM UMA LINHA
            df_stats = pd.json_normalize(
                data,
                record_path=["teamGameStats"],
            )
            df_stats = pd.DataFrame(
                {
                    f"away_{row['category']}": [row["awayValue"]]
                    for _, row in df_stats.iterrows()
                }
            ).join(
                pd.DataFrame(
                    {
                        f"home_{row['category']}": [row["homeValue"]]
                        for _, row in df_stats.iterrows()
                    }
                )
            )
            df_stats["filename"] = file

            df = (
                df_refs.merge(df_final_score, on="filename", how="outer")
                .merge(df_period_score, on="filename", how="outer")
                .merge(df_period_shots, on="filename", how="outer")
                .merge(df_stats, on="filename", how="outer")
            )

            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
        except Exception as e:
            print(f"Error --- {e}")


def append_game_details():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "game_details"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_game_details/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_game_details(file):
    try:
        update_table_with_pk(
            file,
            "raw_game_details",
            "filename",
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_game_details")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_game_details/landing"
    JSON_DIR = "data/json_data/raw_game_details"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_game_details/staging"
    CSV_DIR = "data/csv_data/raw/raw_game_details"
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
        extract_game_details()
        transform_game_details()
        appended_file = append_game_details()

        if appended_file:  # Garante que um arquivo foi criado antes de tentar carregar
            loaded_file = load_game_details(appended_file)

            if (
                loaded_file
            ):  # Garante que o carregamento foi bem-sucedido antes de limpar
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
