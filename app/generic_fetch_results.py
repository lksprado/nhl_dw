import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
from src.logger import logger
import json 

def make_request(url: str):
    """ PROVIDE THE URL TO GET RESULTS"""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"API call returned: {response.status_code}")
            return None
    except requests.RequestException as e:
        logger.error(f"Something wrong with the API call: {e}")
        return None
        
def save_json(file_name: str,data, json_dir):
    """
    SAVES DATA TO JSON
    file_name: 
    """
    try:
        file_path = os.path.join(json_dir, f"raw_{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        logger.info(f"JSON saved at: {file_path}")
    except Exception as e:
            logger.error(f"Failed to save JSON: {e}")
            

#if __name__ == "__main__":
    ## tudo é json
    # nhl = NHL()
    # url_season = 'https://api-web.nhle.com/v1/season'
    # seasons = nhl.make_request(url_season)
    # print(seasons)

    ## RETORNOU UMA LISTA DE DICIONÁRIOS 
    # url_teams = 'https://api.nhle.com/stats/rest/en/team'
    # teams = nhl.make_request(url_teams)
    # print(teams)

    ## RETORNA LISTA DOS JOGADORES POR TIME
    #https://github.com/Zmalski/NHL-API-Reference?tab=readme-ov-file#get-team-roster-by-season