import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.time_tracker import track_time
import pandas as pd
import json
from app.extraction.generic_get_results import make_request, save_json
from multiprocessing import Pool, cpu_count
from app.transforming.generic_df_appenders import df_appender_folder


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
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/player/{player_id}/game-log/{season_id}/{season_type}"
    OUTPUT_DIR = "data/json_data/raw_game_log/landing"

    parameters_input = "app/all_game_logs_till_2025.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(
        by=["season_id", "player_id", "game_type_id"], ascending=False
    )

    pattern = os.path.join(OUTPUT_DIR, "*_*_*.json")
    files_to_skip = glob.glob(pattern)
    existing_combinations = set()
    for file in files_to_skip:
        match = re.search(r"(\d+)_(\d+)_(\d+)\.json", os.path.basename(file))
        if match:
            player_id = match.group(1)
            season_id = match.group(2)
            game_type_id = match.group(3)
            existing_combinations.add((player_id, season_id, game_type_id))

    df_filtered = df_parameter[
        ~df_parameter.apply(
            lambda row: (
                str(row["player_id"]),
                str(row["season_id"]),
                str(row["game_type_id"]),
            )
            in existing_combinations,
            axis=1,
        )
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in df_filtered.itertuples():
            player_id = row.player_id
            season_id = row.season_id
            game_type_id = row.game_type_id
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

        end_time = time.time()
        dif = end_time - start_time
        hours, remainder = divmod(dif, 3600)
        minutes, seconds = divmod(remainder, 60)

        print(f"Done in {int(hours)}h {int(minutes)}m  {int(seconds)}s")


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
    pattern = "data/json_data/raw_game_log/*_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_game_log/staging"
    input_files = glob.glob(pattern)

    with Pool(cpu_count()) as pool:
        pool.starmap(process_single_file, [(file, OUTPUT_DIR) for file in input_files])
    print("Done")


@track_time
def processed_game_log():
    output_file_name = "all_game_log"
    input_csv_dir = "data/csv_data/raw/raw_game_log/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


if __name__ == "__main__":
    # get_game_log()
    # tansform_raw_game_log()
    # processed_game_log()
    # create_and_load_table('data/csv_data/processed/all_game_log.csv','raw_game_log')
    pass
