import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob

from app.transforming.generic_json_parsers import (
    parsing_json_pandas,
    parsing_json_pandas_2,
)


######## SINGLE ##############################################################################################
def raw_current_standings():
    output = "data/csv_data/raw/single"
    input_file = "data/json_data/single/raw_current_standings.json"
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
    input_file = "data/json_data/single/raw_seasons.json"
    parsing_json_pandas(input_file, "data", output)


# def raw_teams():
#     input_file = 'data/api_data/raw_teams.json'
#     parsing_json_pandas(input_file, 'data', output)

######## FOLDERS ##############################################################################################


def raw_stats_current_goalies():
    pattern = "data/json_data/raw_goalie_stats/raw_stats_current_goalies_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_stats_current_goalies")


def raw_stats_current_skaters():
    pattern = "data/json_data/raw_skater_stats/raw_stats_current_skaters_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_stats_current_skaters")


def raw_club_status():
    pattern = "data/json_data/raw_club_stats/raw_stats_club_now_*_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_club_status")


def raw_roster_season():
    pattern = "data/json_data/raw_roster_season/raw_roster_*_*.json"
    input_files = glob.glob(pattern)
    for input_file in input_files:
        parsing_json_pandas_2(input_file, "data/csv_data/raw/raw_roster_season")


# def raw_team_season():
#     pattern  = 'data/api_data/raw_team_season_*.json'
#     input_files = glob.glob(pattern)
#     for input_file in input_files:
#         parsing_json_pandas(input_file,None,output)


# def raw_roster_season_2():
#     pattern  = 'data/json_data/raw_roster_season/raw_roster_*_*.json'
#     input_files = glob.glob(pattern)
#     for input_file in input_files:
#         parsing_json_pandas_2(input_file,'data/csv_data/raw/raw_roster_season')

if __name__ == "__main__":
    # raw_current_standings()
    # raw_season_info()
    # raw_stats_current_goalies()
    # raw_stats_current_skaters()
    # raw_club_status()
    # raw_roster_season()
    # raw_all_games_info()
    raw_seasons()

    # raw_teams()
    # raw_seasons()
    # raw_team_season()
    # raw_roster_season_2()
