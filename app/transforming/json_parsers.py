from multiprocessing import Pool, cpu_count

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from utils.time_tracker import track_time
import glob

from app.transforming.generic_json_parsers import (
    parsing_json_pandas,
    parsing_json_pandas_2,
    parsing_json_pandas_3,
    parsing_json_pandas_4,
    parsing_json_pandas_5,
)


######## SINGLE ##############################################################################################
def raw_current_standings():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_standings_now.json"
    parsing_json_pandas(input_file, "standings", output)


def raw_season_info():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_season_info.json"
    parsing_json_pandas(input_file, "data", output)


def raw_all_games_info():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_all_games_info.json"
    parsing_json_pandas(input_file, "data", output)


def raw_seasons():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_seasons_id.json"
    parsing_json_pandas(input_file, "data", output)


def raw_teams():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_teams.json"
    parsing_json_pandas(input_file, "data", output)


@track_time
def raw_game_info():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_game_info.json"
    parsing_json_pandas_5(input_file, output)


######## FOLDERS ##############################################################################################


def raw_goalie_stats():
    pattern = "data/json_data/raw_goalie_stats/raw_stats_goalies_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_stats_goalies")


def raw_stats_skaters():
    pattern = "data/json_data/raw_skater_stats/raw_stats_skaters_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_stats_skaters")


def raw_club_stats():
    pattern = "data/json_data/raw_club_stats/raw_stats_club_*_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_club_stats")


def raw_roster_season():
    pattern = "data/json_data/raw_roster_season/raw_roster_*_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_roster_season")


def raw_team_season():
    pattern = "data/json_data/raw_roster_season/raw_team_season_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_team_season")


def raw_all_skater_stats():
    pattern = "data/json_data/raw_all_skater_stats/raw_stats_all_skaters_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_all_skater_stats")


def raw_all_goalie_stats():
    pattern = "data/json_data/raw_all_goalies_stats/raw_stats_all_goalies_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_all_goalies_stats")


def raw_all_team_stats():
    pattern = "data/json_data/raw_all_team_stats/raw_stats_all_teams_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_all_team_stats")


def raw_game_log():
    pattern = "data/json_data/raw_game_log/raw_*_*_2.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        try:
            parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_game_log")
        finally:
            continue
    print("Done")


def raw_player_info():
    pattern = "data/json_data/raw_player_info/raw_player_*_info.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        try:
            parsing_json_pandas_3(input_file, "data/csv_data/raw/raw_player_info")
        finally:
            continue


def process_file(input_file):
    try:
        # Chama a função de conversão diretamente
        parsing_json_pandas_4(input_file, "data/csv_data/raw/raw_play_by_play")
        print(f"Processado: {input_file}")
    except Exception as e:
        print(f"Erro ao processar {input_file}: {e}")


@track_time
def raw_play_by_play():
    pattern = "data/json_data/raw_play_by_play/raw_*.json"
    input_files = glob.glob(pattern)

    # Multiprocessing para processar vários arquivos em paralelo
    with Pool(cpu_count()) as pool:
        pool.map(process_file, input_files)


if __name__ == "__main__":
    ## SINGLE ###############
    # raw_current_standings()
    # raw_season_info()
    # raw_all_games_info()
    # raw_seasons()
    # raw_teams()
    raw_game_info()

    ## FOLDER ###############
    # raw_goalie_stats()
    # raw_stats_skaters()
    # raw_club_stats()
    # raw_roster_season()
    # raw_team_season()
    # raw_all_skater_stats()
    # raw_all_goalie_stats()
    # raw_all_team_stats()
    # raw_game_log()
    # raw_player_info()
    # process_file('teste/raw_2024020748.json')
    # raw_play_by_play()
# pass
