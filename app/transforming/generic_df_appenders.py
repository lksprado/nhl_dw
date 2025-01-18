import pandas as pd 
import glob

def df_appender_folder(file_name,input_csv_dir,output_dir):
    """
    Appends all csv files in a folder and saves as csv \n
    input_csv_dir:
        Provide the folder with csv files
    output_dir:
        Provide output folder for csv file
    """
    all_files = glob.glob(input_csv_dir + "/*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(f'{output_dir}/{file_name}.csv', index=False)
    
