import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))) 
import polars as pl
from src.logger import logger
import pandas as pd 
from utils.time_tracker import track_time

@track_time
def parsing_json_pandas (filename:str, ls, output_dir:str, rename:list = None):
    """
    Parses json and saves as csv using Pandas lib
    filename:
        Provide the json file
    ls:
        Provide desired json list element
    output_dir:
        Provide output folder for csv file
        
    """
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace('.json','.csv')
    data = pd.read_json(filename)
    if isinstance(data, pd.DataFrame):
        parsed_data = data 
    elif isinstance(data, pd.Series):
        parsed_data = pd.json_normalize(data[ls])
        
    if rename:
        if len(rename) != len(parsed_data.columns):
            raise ValueError("The 'rename' list must match the number of columns in the DataFrame.")
        parsed_data.columns = rename

    parsed_data.to_csv(f'{output_dir}/{csv_filename}', index=False)


@track_time
def parsing_json_polars(filename:str, ls:str, output_dir:str):
    """
    Parses json and saves as csv using Polars lib
    filename:
        Provide the json file
    ls:
        Provide desired json list element
    output_dir:
        Provide output folder for csv file
    """
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace('.json','.csv')

    data = pl.read_json(filename)
    df = pl.DataFrame(data[ls]).explode(ls)
    
    def unnest_all(df, separator="."):
        def _unnest_all(struct_columns):
            return df.with_columns(
                [
                    pl.col(col).struct.rename_fields(
                        [
                            f"{col}{separator}{field_name}"
                            for field_name in df[col].struct.fields
                        ]
                    )
                    for col in struct_columns
                ]
            ).unnest(struct_columns)

        struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct]
        while len(struct_columns):
            df = _unnest_all(struct_columns=struct_columns)
            struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct]
        return df

    # Desanexar as estruturas
    df_unnested = unnest_all(df)
    df_unnested.columns = [col.replace(f"{ls}.", '') for col in df_unnested.columns]
    df_unnested.write_csv(f'{output_dir}/{csv_filename}')

##########################################################################################################################

output = "data/csv_data/raw"
def raw_current_standings():
    file = "data/api_data/raw_current_standings.json"
    parsing_json_pandas(file,'standings',output)

def raw_season_info():
    file = 'data/api_data/raw_season_info.json'
    parsing_json_pandas(file, 'data', output)

def raw_stats_current_goalies():
    file = 'data/api_data/raw_stats_current_goalies.json'
    parsing_json_pandas(file, 'wins', output)

def raw_stats_current_goals_per_player():
    file = 'data/api_data/raw_stats_current_goals_per_player.json'
    parsing_json_pandas(file, 'goals', output)

def raw_teams():
    file = 'data/api_data/raw_teams.json'
    parsing_json_pandas(file, 'data', output)

def raw_seasons():
    file = 'data/api_data/raw_seasons.json'
    parsing_json_pandas(file,None,output,['season_id'])
    
    
if __name__ == "__main__":
    # raw_current_standings()
    # raw_season_info()
    # raw_stats_current_goalies()
    # raw_stats_current_goals_per_player()
    # raw_teams()
    raw_seasons()