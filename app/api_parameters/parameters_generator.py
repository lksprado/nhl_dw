from app.extraction.generic_get_results import make_request
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()


def generate_seasonid_list():
    URL = "https://api-web.nhle.com/v1/season"
    OUTPUT_DIR = "app/api_parameters/"
    data, _ = make_request(URL)
    df = pd.DataFrame({"season_id": data})
    output_file = f"{OUTPUT_DIR}season_ids.csv"
    df.to_csv(output_file, index=False)
    print(f"Arquivo CSV salvo em: {output_file}")


def generate_seasonid_gametypeid_list():
    URL = "https://api-web.nhle.com/v1/season"
    OUTPUT_DIR = "app/api_parameters/"
    data, _ = make_request(URL)
    df = pd.DataFrame({"season_id": data})
    df_reg = df.copy()
    df_reg["game_type_id"] = 2
    df_pos = df.copy()
    df_pos["game_type_id"] = 3
    df_final = pd.concat([df_reg, df_pos], ignore_index=True)
    output_file = f"{OUTPUT_DIR}season_ids_gametypes_ids.csv"
    df_final.to_csv(output_file, index=False)
    print(f"Arquivo CSV salvo em: {output_file}")


def generate_seasonid_teamid_gametypeid_list():
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
    cursor.execute(
        "SELECT * FROM nhl_raw.stg_csv__parameters_team_seasons_types where team_id is not null and season_id is not null and game_type is not null"
    )
    results = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)
    df.to_csv("app/api_parameters/seasonid_teamid_game_type.csv", index=False)

    cursor.close()
    conn.close()


def generate_active_players_list():
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

    query = """
    WITH
    latest_season AS (
        SELECT
        max(REPLACE(featuredstats_season,'.0','')::int) AS max_season
        FROM nhl_raw.raw_player_info rpi
        )
    SELECT
    playerid
    FROM nhl_raw.raw_player_info
    INNER JOIN latest_season ls
    ON REPLACE(featuredstats_season,'.0','')::int = ls.max_season;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)
    df.to_csv("app/api_parameters/active_playerid.csv", index=False)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    # generate_seasonid_list()
    # generate_seasonid_gametypeid_list()
    # generate_seasonid_teamid_gametypeid_list()
    generate_active_players_list()
