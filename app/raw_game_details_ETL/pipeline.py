import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from app.extraction.generic_get_results import make_request, save_json
import json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import copy_with_upsert

load_dotenv()


## raw_game_details
def fetch_and_save_game_details(game_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"{game_id}_details", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


## raw_game_details
def extract_game_details():
    """
    #### Daily Update
    ---
    """
    URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail"
    OUTPUT_DIR = "data/json_data/raw_game_details/landing"

    ## CONNECT TO
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM nhl_raw.stg_csv__gameid_to_get")
    results = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)

    cursor.close()
    conn.close()
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
                print(f"Fetched --- {game_id}")
            except Exception as e:
                print(f"Exception occurred: {e}")


def transform_game_details():
    INPUT_DIR = "data/json_data/raw_game_details/landing"
    OUTPUT_DIR = "data/csv_data/raw/raw_game_details/staging"
    json_dir = os.listdir(INPUT_DIR)

    for file in json_dir:
        j = os.path.join(INPUT_DIR, file)
        try:
            csv_filename = os.path.basename(j)
            csv_filename = csv_filename.replace(".json", ".csv")

            with open(j) as f:
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

            df.to_csv(f"{OUTPUT_DIR}/{csv_filename}", index=False)
            print(f"Arquivo salvo: {csv_filename}")
        except Exception as e:
            print(f"Something went wrong --- {e}")


def append_game_details():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "game_details"
    input_csv_dir = "data/csv_data/raw/raw_game_details/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


def load_game_details():
    copy_with_upsert(
        "data/csv_data/processed/game_details_2025-02-07.csv", "raw_game_details"
    )


if __name__ == "__main__":
    # extract_game_details()
    #  transform_game_details()
    # append_game_details()
    # load_game_details()
    pass
