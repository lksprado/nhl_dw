# UPDATE DAILY

# Get Team Stats

#     Endpoint: /{lang}/team/{report}
#     Method: GET
#     Description: Retrieve team stats for a specific report.
#     Parameters:
#         report (string) - Team report
#         lang (string) - Language code
#     Request Parameters:
#         isAggregate (query, boolean) - Optional
#         isGame (query, boolean) - Optional
#         factCayenneExp (query, string) - Optional
#         include (query, string) - Optional
#         exclude (query, string) - Optional
#         cayenneExp (query, string) - Optional
#         sort (query, string) - Optional
#         dir (query, string) - Optional
#         start (query, int) - Optional
#         limit (query, int) - Optional (Note: a limit of -1 will return all results)
#     Response: JSON format


import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.generic_fetch_results import make_request, save_json

DIR = "./api_data"

URL = 'https://api.nhle.com/stats/rest/en/team/summary?sort=shotsForPerGame&cayenneExp=seasonId=20242025%20and%20gameTypeId=2'

def fetch_seasons(url, file_name: str, json_dir):
    data = make_request(url)
    save_json(file_name, data, json_dir)

if __name__=="__main__":
    fetch_seasons(URL,"teams", DIR)


