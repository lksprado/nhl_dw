# THE SCRIPTS IS TO GATHER DATA FROM THE FIRST FILES TO APPLY THEM AS PARAMETERS IN THE API

import polars as pl 
def get_column(param_name:str,col: str, csv_filename:str):
    
    df = pl.read_csv(csv_filename)
    unique_values = set(df[col])
    new_df = pl.DataFrame({
        col: list(unique_values),
        "api_parameter": [param_name] * len(unique_values)
    })
    return new_df
    #new_df.write_csv(f'{output_dir}/{param_name}.csv')

###########################################################################

csv_file = 'data/csv_data/raw/raw_seasons.csv'
df1 = get_column('season_id','season_id', csv_file)

csv_file = 'data/csv_data/raw/raw_seasons.csv'
df2 = get_column('team_id','triCode', csv_file)


## PARAMETERS
# player_id
# season_id
# game-type: 1 for pre-season(?), 2 for regular, 3 for playoffs
# Date: YYYY-MM-DD
# team_id
# 