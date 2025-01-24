import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from app.transforming.generic_df_appenders import df_appender_folder
from utils.time_tracker import track_time


def processed_all_goalies_stats():
    output_file_name = "all_goalies_stats"
    input_csv_dir = "data/csv_data/raw/raw_all_goalies_stats"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


def processed_all_skaters_stats():
    output_file_name = "all_skaters_stats"
    input_csv_dir = "data/csv_data/raw/raw_all_skater_stats"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


def processed_all_team_stats():
    output_file_name = "all_team_stats"
    input_csv_dir = "data/csv_data/raw/raw_all_team_stats"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


def processed_club_stats():
    output_file_name = "all_club_stats"
    input_csv_dir = "data/csv_data/raw/raw_club_stats"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_game_log():
    output_file_name = "all_game_log"
    input_csv_dir = "data/csv_data/raw/raw_game_log"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_player_info():
    output_file_name = "player_info"
    input_csv_dir = "data/csv_data/raw/raw_player_info"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_roster_season():
    output_file_name = "roster_season"
    input_csv_dir = "data/csv_data/raw/raw_roster_season"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_stats_goalies():
    output_file_name = "stats_goalies"
    input_csv_dir = "data/csv_data/raw/raw_stats_goalies"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_stats_skaters():
    output_file_name = "stats_skaters"
    input_csv_dir = "data/csv_data/raw/raw_stats_skaters"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


@track_time
def processed_team_season():
    output_file_name = "team_season"
    input_csv_dir = "data/csv_data/raw/raw_team_season"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


if __name__ == "__main__":
    # processed_all_goalies_stats()
    # processed_all_skaters_stats()
    # processed_all_team_stats()
    # processed_club_stats()
    processed_game_log()
    # processed_player_info()
    # processed_stats_goalies()
    # processed_stats_skaters()
    # processed_team_season()
    # pass
