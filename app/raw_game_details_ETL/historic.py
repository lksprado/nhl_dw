import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import pandas as pd
import json
from loguru import logger
from multiprocessing import Pool, cpu_count
from app.extraction.generic_get_results import make_request, save_json

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


## raw_game_details
def fetch_and_save_game_details(game_id, url, output_dir):
    data, _ = make_request(url)
    logger.info(f"Data collected --- {url}")
    save_json(f"{game_id}_details", data, output_dir)


## raw_game_details
def extract_game_details():
    URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail"
    OUTPUT_DIR = "data/json_data/raw_game_details/landing"

    df = pd.read("app/api_parameters/gamedetails_gameids.csv")

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


def transform_game_details_worker(input_file):
    OUTPUT_DIR = "data/csv_data/raw/raw_game_details"  ## AJUSTAR STAGING
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
        df_home = df_home.pivot(index="filename", columns="period", values="score_home")
        df_away = df_away.pivot(index="filename", columns="period", values="score_away")
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


if __name__ == "__main__":
    input_files = glob.glob("data/json_data/raw_game_details/*_details.json")

    with Pool(cpu_count()) as pool:
        pool.map(transform_game_details_worker, input_files)
