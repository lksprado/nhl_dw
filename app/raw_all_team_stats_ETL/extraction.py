import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## raw_all_team_stats rats
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

    # TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.season_id) for row in df_parameter.itertuples()]
    # for url in urls:
    #     season_id = url.split('/')[-2]
    #     data = make_request(url)
    #     save_json(f"stats_current_goalies_{season_id}", data, OUTPUT_DIR)
