# THE SCRIPTS IS TO GATHER DATA FROM THE FIRST FILES TO APPLY THEM AS PARAMETERS IN THE API

import polars as pl 
#import pandas pd 
import glob
import os 
import pandas as pd 
import re

def get_column(param_name: str, col: str, csv_filename: str):
    df = pl.read_csv(csv_filename)
    unique_values = set(map(str, df[col].to_list()))  # Convert values to string using map
    new_df = pl.DataFrame({
        "api_parameter": [param_name] * len(unique_values),
        "value": list(unique_values)
    })
    return new_df

def seasons_per_team():
    pattern  = 'data/csv_data/raw/raw_team_season_*.csv'
    files = glob.glob(pattern)
    all_data = []
    for file in files:
        df = pl.read_csv(file)
        team_id = os.path.basename(file).replace('raw_team_season_', '').replace('.csv', '')
        seasons = set(map(str, df['season'].to_list()))
        new_df = pl.DataFrame({
            "team_id": [team_id] * len(seasons),
            "season_id": list(seasons)
        })
        all_data.append(new_df)
    all_data = pl.concat(all_data)    
    all_data.write_csv('data/csv_data/parameters_team_season.csv')
    
def players_id():
    pattern  = 'data/csv_data/raw/raw_roster_season/raw_roster_*_*.csv'
    files = glob.glob(pattern)
    all_data = []
    for file in files:
        season_id = re.findall(r'\d+', os.path.basename(file))[0]
        team_id = re.findall(r'raw_roster_([A-Z]{3})_', os.path.basename(file))[0]
        try:
            df = pl.read_csv(file, has_header=True)

            if 'id' in df.columns:
                players = set(map(str, df['id'].to_list()))
                new_df = pl.DataFrame({
                    "player_id": list(players),
                    "season_id": [season_id] * len(players),
                    "team_id": [team_id] * len(players)
                })
                all_data.append(new_df)
            else:
                print(f"Coluna 'id' não encontrada no arquivo: {file}")
        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")

    if all_data:
        all_data = pl.concat(all_data)
        all_data.write_csv('data/csv_data/processed/parameters_players.csv')
        print("Arquivo 'parameters_players.csv' salvo com sucesso.")
    else:
        print("Nenhum dado processado.")
    
def players_id_remaining():
    pattern = 'data/json_data/raw_game_log_*_*_2.json'
    files_to_skip = glob.glob(pattern)
    season_id = re.findall(r'\d+', os.path.basename(files_to_skip))[0]
    team_id = re.findall(r'raw_roster_([A-Z]{3})_', os.path.basename(files_to_skip))[0]
    pass
    
    
###########################################################################
if __name__ == "__main__":
    players_id()



## PARAMETERS
# player_id
# season_id
# game-type: 1 for pre-season(?), 2 for regular, 3 for playoffs
# Date: YYYY-MM-DD
# team_id
# game-id
# will look like this: 2023020001
# The first 4 digits identify the season of the game (ie. 2017 for the 2017-2018 season). 
# Always refer to a season with the starting year. 
# A game played in March 2018 would still have a game ID that starts with 2017
# The next 2 digits give the type of game, where 01 = preseason, 02 = regular season, 03 = playoffs, 04 = all-star
# The final 4 digits identify the specific game number. 
# For regular season and preseason games, this ranges from 0001 to the number of games played. 
# (1353 for seasons with 32 teams (2022 - Present), 1271 for seasons with 31 teams (2017 - 2020) and 1230 for seasons with 30 teams). 
# For playoff games, the 2nd digit of the specific number gives the round of the playoffs, the 3rd digit specifies the matchup, and the 4th digit specifies the game (out of 7).