import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from app.extraction.generic_get_results import make_request, save_json

######## SINGLE #############################################################################
#############################################################################################
#############################################################################################


def get_all_games_info():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/game
    Method: GET
    Description: Retrieve game information.
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/season"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("all_games_info", data, OUTPUT_DIR)


def get_season_ids():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/season
    Method: GET
    Description: Retrieve a list of all season IDs past & present in the NHL.
    Response: JSON format
    """
    URL = "https://api-web.nhle.com/v1/season"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("season_ids", data, OUTPUT_DIR)


def get_season_info():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/season
    Method: GET
    Description: Retrieve season information.
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/season"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("season_info", data, OUTPUT_DIR)


def get_standings_now():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/standings
    Method: GET
    Description: Retrieve current standings.
    Request Parameters:
        expand (query, string) - Optional
    Response: JSON format
    """
    URL = "https://api-web.nhle.com/v1/standings/now"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("standings_now", data, OUTPUT_DIR)


def get_teams():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/team
    Method: GET
    Description: Retrieve list of all teams.
    Parameters:
        lang (string) - Language code
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/team"
    OUTPUT_DIR = "data/json_data/single"

    data, _ = make_request(URL)
    save_json("teams", data, OUTPUT_DIR)


######## FOLDER #############################################################################
###########################################S##################################################
#############################################################################################


def get_teams_stats(season_type=2):
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/team/{report}
    Method: GET
    Description: Retrieve team stats for a specific report.
    Parameters:
        report (string) - Team report
        lang (string) - Language code
    Request Parameters:
        isAggregate (query, boolean) - Optional
        isGame (query, boolean) - Optional
        factCayenneExp (query, string) - Optional
        include (query, string) - Optional
        exclude (query, string) - Optional
        cayenneExp (query, string) - Optional
        sort (query, string) - Optional
        dir (query, string) - Optional
        start (query, int) - Optional
        limit (query, int) - Optional (Note: a limit of -1 will return all results)
    Response: JSON format
    """

    URL = "https://api.nhle.com/stats/rest/en/team/summary?seasonId={season_id}&TypeId={season_step}"
    OUTPUT_DIR = "data/json_data/raw_team_stats"

    parameters_input = "data/csv_data/processed/parameters.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[df_parameter["api_parameter"] == "season_id"]

    # TO GET CURRENT DATA
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id, season_step=season_type)
    data, _ = make_request(url)
    save_json(f"stats_all_teams_{max_season_id}", data, OUTPUT_DIR)

    # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.value, season_step = season_type) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('seasonId=')[1].split('&')[0]
    #     data, _ = make_request(url)
    #     save_json(file_name=f"stats_all_teams_{season_id}",data=data, output_json_dir=OUTPUT_DIR)


def get_current_goalie_stats_leaders():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/goalie-stats-leaders/current
    Method: GET
    Description: Retrieve current goalie stats leaders.
    Request Parameters:
        categories (query, string) - Optional
        limit (query, int) - Optional (Note: a limit of -1 will return all results)
    Response: JSON format
    """
    URL = "https://api-web.nhle.com/v1/goalie-stats-leaders/{season_id}/2?limit=-1"
    OUTPUT_DIR = "data/json_data/raw_goalie_stats"

    parameters_input = "data/csv_data/processed/parameters.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[df_parameter["api_parameter"] == "season_id"]

    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"stats_goalies_{max_season_id}", data, OUTPUT_DIR)

    # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.season_id) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('/')[-2]
    #     data = make_request(url)
    #     save_json(f"stats_current_goalies_{season_id}", data, OUTPUT_DIR)


def get_current_skater_stats_leaders():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/skater-stats-leaders/current
    Method: GET
    Description: Retrieve current skater stats leaders.
    Parameters:
        categories (query, string) - Optional
        limit (query, int) - Optional (Note: a limit of -1 will return all results)
    Response: JSON format
    """
    URL = "https://api-web.nhle.com/v1/skater-stats-leaders/{season_id}/2?&limit=-1"
    OUTPUT_DIR = "data/json_data/raw_skater_stats"

    parameters_input = "data/csv_data/processed/parameters.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[df_parameter["api_parameter"] == "season_id"]

    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"stats_skaters_{max_season_id}", data, OUTPUT_DIR)

    # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.season_id) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('/')[-2]
    #     data = make_request(url)
    #     save_json(f"stats_current_skaters_{season_id}", data, OUTPUT_DIR)


def get_team_roster_by_season():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/roster/{team}/{season}
    Method: GET
    Description: Retrieve the roster for a specific team and season.
    Parameters:
        team (string) - Three-letter team code
        season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
    Response: JSON format
    """

    # BUILD URLS
    URL = "https://api-web.nhle.com/v1/roster/{team_id}/{season_id}"
    OUTPUT_DIR = "data/json_data/raw_roster_season"

    parameters_input = "data/csv_data/processed/parameters_team_season.csv"
    df_parameter = pd.read_csv(parameters_input)

    urls = [
        URL.format(team_id=row.team_id, season_id=row.season_id)
        for row in df_parameter.itertuples()
    ]

    # LOOP THROUGH URLS
    for url in urls:
        team_id = url.split("/")[-2]
        season_id = url.split("/")[-1]
        file_name = f"roster_{team_id}_{season_id}"

        data, _ = make_request(url)
        save_json(file_name, data, OUTPUT_DIR)


def get_club_stats_for_the_season_for_a_team():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/club-stats-season/{team}
    Method: GET
    Description: Returns an overview of the stats for each season for a specific club. Seems to only indicate the gametypes played in each season.
    Parameters:
        team (string) - Three-letter team code
        season_type (int) - Type of the season (default is 2)
    Response: JSON format
    """
    INPUT_FILE = "data/csv_data/raw/single/raw_teams.csv"
    OUTPUT_DIR = "data/json_data/raw_team_season"

    ls = pd.read_csv(INPUT_FILE)
    ls = ls["triCode"]
    for team in ls:
        URL = f"https://api-web.nhle.com/v1/club-stats-season/{team}"
        OUTPUT_DIR = "data/json_data/raw_team_season"

        data, _ = make_request(URL)
        save_json(f"team_season_{team}", data, OUTPUT_DIR)


def get_club_stats_now(season_type=2):
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/club-stats/{team}/now
    Method: GET
    Description: Retrieve current statistics for a specific club.
    Parameters:
        team (string) - Three-letter team code
        season_type (int) - Type of the season (default is 2)
    Response: JSON format
    """

    URL = "https://api-web.nhle.com/v1/club-stats/{team_id}/{season_id}/{season_step}"
    OUTPUT_DIR = "data/json_data/raw_club_stats"

    parameters_input = "data/csv_data/processed/parameters_team_season.csv"
    df_parameter = pd.read_csv(parameters_input)
    max_season_id = df_parameter["season_id"].max()
    df_filtered = df_parameter[df_parameter["season_id"] == max_season_id]
    unique_teams = df_filtered["team_id"].unique()

    for team_id in unique_teams:
        url = URL.format(
            team_id=team_id, season_id=max_season_id, season_step=season_type
        )
        data, _ = make_request(url)
        save_json(
            f"stats_club_{team_id}_{max_season_id}_{season_type}", data, OUTPUT_DIR
        )

    # TO GET HISTORIC DATA
    # urls = [URL.format(team_id=row.team_id, season_id=row.season_id, season_step = season_type) for row in df_parameter.itertuples()]
    # for url in urls:
    #     team_id = url.split('/')[-3]
    #     season_id = url.split('/')[-2]
    #     file_name = f"stats_club_now_{team_id}_{season_id}_{season_step}"
    #     data, _ = make_request(url)
    #     save_json(file_name, data, OUTPUT_DIR)
    #     print(f"Data fetched for {team_id} in season {season_id}")


def fetch_and_save_game_log(player_id, season_id, season_step, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"{player_id}_{season_id}_{season_step}", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


def get_game_log(season_type=2):
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/player/{player}/game-log/{season}/{game-type}
    Method: GET
    Description: Retrieve the game log for a specific player, season, and game type.
    Parameters:
        player (int) - Player ID
        season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
        game-type (int) - Game type (guessing 2 for regular season, 3 for playoffs)
    Response: JSON format
    """
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/player/{player_id}/game-log/{season_id}/{season_type}"
    OUTPUT_DIR = "data/json_data/raw_game_log"

    parameters_input = "data/csv_data/processed/parameters_players.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(
        by=["season_id", "player_id"], ascending=False
    )

    pattern = os.path.join(OUTPUT_DIR, "raw_game_log_*_*_2.json")
    files_to_skip = glob.glob(pattern)
    existing_combinations = set()
    for file in files_to_skip:
        match = re.search(r"raw_game_log_(\d+)_(\d+)_2\.json", os.path.basename(file))
        if match:
            player_id = match.group(1)
            season_id = match.group(2)
            existing_combinations.add((player_id, season_id))

    df_filtered = df_parameter[
        ~df_parameter.apply(
            lambda row: (str(row["player_id"]), str(row["season_id"]))
            in existing_combinations,
            axis=1,
        )
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in df_filtered.itertuples():
            player_id = row.player_id
            season_id = row.season_id
            url = URL.format(
                player_id=player_id, season_id=season_id, season_type=season_type
            )
            futures.append(
                executor.submit(
                    fetch_and_save_game_log,
                    player_id,
                    season_id,
                    season_type,
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


def fetch_and_save_player_info(player_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"player_{player_id}_info", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


def get_player_info():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /v1/player/{player}/game-log/{season}/{game-type}
    Method: GET
    Description: Retrieve the game log for a specific player, season, and game type.
    Parameters:
        player (int) - Player ID
        season (int) - Season in YYYYYYYY format, where the first four digits represent the start year of the season, and the last four digits represent the end year.
        game-type (int) - Game type (guessing 2 for regular season, 3 for playoffs)
    Response: JSON format
    """
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/player/{player_id}/landing"

    OUTPUT_DIR = "data/json_data/raw_player_info"

    parameters_input = "data/csv_data/processed/parameters_players.csv"
    df_parameter = pd.read_csv(parameters_input)

    pattern = os.path.join(OUTPUT_DIR, "player_*_info.json")
    files_to_skip = glob.glob(pattern)
    existing_combinations = set()
    for file in files_to_skip:
        match = re.search(r"player_(\d+)_info\.json", os.path.basename(file))
        if match:
            player_id = match.group(1)
            existing_combinations.add((player_id))

    df_filtered = df_parameter[
        ~df_parameter.apply(
            lambda row: str(row["player_id"]) in existing_combinations, axis=1
        )
    ]

    df_filtered = df_filtered["player_id"].unique()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for player_id in df_filtered:
            url = URL.format(player_id=player_id)
            futures.append(
                executor.submit(fetch_and_save_player_info, player_id, url, OUTPUT_DIR)
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


def get_skater_stats():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/skater/{report}
    Method: GET
    Description: Retrieve skater stats for a specific report.
    Parameters:
        report (string) - Skater report
        lang (string) - Language code
    Request Parameters:
        isAggregate (query, boolean) - Optional
        isGame (query, boolean) - Optional
        factCayenneExp (query, string) - Optional
        include (query, string) - Optional
        exclude (query, string) - Optional
        cayenneExp (query, string) - Required
        sort (query, string) - Optional
        dir (query, string) - Optional
        start (query, int) - Optional
        limit (query, int) - Optional (Note: a limit of -1 will return all results)
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=seasonId={season_id}"
    OUTPUT_DIR = "data/json_data/raw_all_skater_stats"

    parameters_input = "data/csv_data/processed/parameters.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[df_parameter["api_parameter"] == "season_id"]

    # TO GET CURRENT DATA
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"stats_all_skaters_{max_season_id}", data, OUTPUT_DIR)

    # # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.value) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('=')[-1]
    #     data = make_request(url)
    #     save_json(f"stats_all_skaters_{season_id}", data, OUTPUT_DIR)


def get_goalie_stats():
    """
    #### Daily Update
    ---
    #### API Endpoint Documentation
    Endpoint: /{lang}/skater/{report}
    Method: GET
    Description: Retrieve skater stats for a specific report.
    Parameters:
        report (string) - Skater report
        lang (string) - Language code
    Request Parameters:
        isAggregate (query, boolean) - Optional
        isGame (query, boolean) - Optional
        factCayenneExp (query, string) - Optional
        include (query, string) - Optional
        exclude (query, string) - Optional
        cayenneExp (query, string) - Required
        sort (query, string) - Optional
        dir (query, string) - Optional
        start (query, int) - Optional
        limit (query, int) - Optional (Note: a limit of -1 will return all results)
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&cayenneExp=seasonId={season_id}"
    OUTPUT_DIR = "data/json_data/raw_all_goalies_stats"

    parameters_input = "data/csv_data/processed/parameters.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[df_parameter["api_parameter"] == "season_id"]

    # TO GET CURRENT DATA
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"stats_all_goalies_{max_season_id}", data, OUTPUT_DIR)

    # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.value) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('=')[-1]
    #     data = make_request(url)
    #     save_json(f"stats_all_goalies_{season_id}", data, OUTPUT_DIR)


def fetch_and_save_play_by_play(game_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"{game_id}", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


def get_play_by_play():
    """
    #### Daily Update
    ---
    """
    start_time = time.time()

    URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    OUTPUT_DIR = "data/json_data/raw_play_by_play"

    parameters_input = "data/csv_data/processed/parameters_gameid.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(by=["game_id"], ascending=False)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in df_parameter.itertuples():
            game_ids = row.game_id
            url = URL.format(game_id=game_ids)
            futures.append(
                executor.submit(
                    fetch_and_save_play_by_play,
                    game_ids,
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


if __name__ == "__main__":
    get_play_by_play()
