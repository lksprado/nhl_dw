import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd

from app.extraction.generic_get_results import make_request, save_json


## raw_all_skaters_stats
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
