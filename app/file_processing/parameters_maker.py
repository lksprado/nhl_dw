# THE SCRIPTS IS TO GATHER DATA FROM THE FIRST FILES TO APPLY THEM AS PARAMETERS IN THE API

import polars as pl 

def get_column(param_name: str, col: str, csv_filename: str):
    df = pl.read_csv(csv_filename)
    unique_values = set(map(str, df[col].to_list()))  # Convert values to string using map
    new_df = pl.DataFrame({
        "api_parameter": [param_name] * len(unique_values),
        "value": list(unique_values)
    })
    return new_df

###########################################################################

csv_file = 'data/csv_data/raw/raw_seasons.csv'
df1 = get_column('season_id', 'season_id', csv_file)

csv_file = 'data/csv_data/raw/raw_teams.csv'
df2 = get_column('team_id', 'triCode', csv_file)

df3 = pl.concat([df1, df2])
df3.write_csv('data/csv_data/parameters.csv')


## PARAMETERS
# player_id
# season_id
# game-type: 1 for pre-season(?), 2 for regular, 3 for playoffs
# Date: YYYY-MM-DD
# team_id
# 