import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

#### GERAM SOMENTE UM ARQUIVO NA PASTA /media/lucas/Files/2.Projetos/nhl-dw/data/json_data/single


#### OPCIONAL SE IMPLENTAR A SEASON_INFO QUE É MAIS COMPLETA
# def get_season_ids():
#     """
#     Returns a list of all seasons
#     """
#     URL = "https://api-web.nhle.com/v1/season"
#     OUTPUT_DIR = "data/json_data/single"

#     data, _ = make_request(URL)
#     save_json("season_ids", data, OUTPUT_DIR)


#### NAO MODELADA - MAS PODE SER ÚTIL PARA CRIAR DIMENSÃO DE SEASONS
# def get_season_info():
#     """
#     Returns all seasons details like start/end dates, number of regular and post season games
#     """
#     URL = "https://api.nhle.com/stats/rest/en/season"
#     OUTPUT_DIR = "data/json_data/single"

#     data, _ = make_request(URL)
#     save_json("season_info", data, OUTPUT_DIR)


### NAO MODELADA
# def get_standings_now():
#     """
#     #### Daily Update
#     Returns current stats for each team
#     """
#     URL = "https://api-web.nhle.com/v1/standings/now"
#     OUTPUT_DIR = "data/json_data/single"

#     data, _ = make_request(URL)
#     save_json("standings_now", data, OUTPUT_DIR)
