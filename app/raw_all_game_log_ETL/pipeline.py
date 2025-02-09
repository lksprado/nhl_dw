import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob
from datetime import date
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.time_tracker import track_time
import pandas as pd
import json
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk


## raw_all_game_log
def fetch_and_save_game_log(player_id, season_id, season_step, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"{player_id}_{season_id}_{season_step}", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


def extract_game_log():
    """
    Pegar a lista de jogadores da atual temporada, season type e atualizar com update
    """

    URL = "https://api-web.nhle.com/v1/player/{player_id}/game-log/{season_id}/{season_type}"
    OUTPUT_DIR = "data/json_data/raw_game_log/landing"

    df_players = pd.read_csv("app/api_parameters/active_playerid.csv")
    df_team_season = pd.read_csv("app/api_parameters/seasonid_teamid_game_type.csv")
    df_team_season = df_team_season[["season_id", "game_type"]].drop_duplicates()

    max_season_id = df_team_season["season_id"].max()
    max_game_type = df_team_season["game_type"].max()

    # Filtra primeiro pelo max_season_id
    df_filtered = df_team_season[df_team_season["season_id"] == max_season_id]

    # Agora pega o maior game_type dentro do max_season_id
    max_game_type = df_filtered["game_type"].max()

    # Filtra novamente considerando ambos os critérios
    df_team_season = df_filtered[df_filtered["game_type"] == max_game_type]

    df_filtered = df_players.merge(df_team_season, how="cross")

    df_filtered.to_csv("app/raw_all_game_log_ETL/teste.csv")

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
                print(f"Exception occurred: {e}")


def process_single_file(input_file, OUTPUT_DIR):
    """
    Processa um único arquivo JSON e salva o resultado como CSV.
    """
    try:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join(OUTPUT_DIR, file)
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
        print(f"Arquivo salvo: {output_file}")
    except Exception as e:
        print(f"Erro ao processar arquivo {input_file}: {e}")


def tansform_raw_game_log():
    """
    Processa todos os arquivos JSON no diretório de entrada e os converte para CSV.
    """
    pattern = "data/json_data/raw_game_log/landing/*_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_game_log/staging"
    input_files = glob.glob(pattern)

    with Pool(cpu_count()) as pool:
        pool.starmap(process_single_file, [(file, OUTPUT_DIR) for file in input_files])
    print("Done")


@track_time
def append_game_log():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "all_game_log"
    input_csv_dir = "data/csv_data/raw/raw_game_log/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


def load_game_log():
    # create_and_load_table_with_sk('data/csv_data/processed/all_game_log.csv','raw_game_log',['gameid','filename'])
    update_table_with_sk(
        "data/csv_data/processed/all_game_log_2025-02-09.csv",
        "raw_game_log",
        ["gameid", "filename"],
    )


if __name__ == "__main__":
    # extract_game_log()
    # tansform_raw_game_log()
    # append_game_log()
    load_game_log()
    pass
