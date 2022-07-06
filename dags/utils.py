from datetime import datetime, timedelta
import os
import requests
import pandas as pd 
import lxml
from datetime import datetime
from bs4 import BeautifulSoup as bs
import os.path


def write_on_txt(file,path):
    with open(path, 'a') as f:
        f.write(f"{str(file)}\n")
def save_csv():
    filename = datetime.now().strftime('%Y-%m-%d-at-%H-%M-%S') +".csv"
    dataframe = get_dataframe()
    dataframe.to_csv(f"/opt/airflow/files/inc/{filename}", index=False)
    write_on_txt(filename, "/opt/airflow/files/inc/inc_log.txt")

def get_dataframe()->pd.DataFrame:
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'}
    url = "https://br.investing.com/currencies/streaming-forex-rates-majors"
    page = requests.get(url, headers=headers)
    df = pd.read_html(page.text)[0]
    df['extracted_at'] = datetime.now().strftime('%Y-%m-%d-at-%H:%M:%S')
    return df

def create_file(file,columns):
    if not os.path.isfile(file):
        pd.DataFrame(columns=columns).to_csv(file,index=False)

def get_last_line(file):
    with open(file,'r') as f:
        last = f.readlines()[-1]
        last = last.strip("\n")
        if last is not None:
            return last
        else:
            return " "

def concat_dataframes():
    final_path = '/opt/airflow/files/final/'
    df_filename = final_path + 'coins.csv'
    log_filename = final_path + 'concat_log.txt'
    inc_path = '/opt/airflow/files/inc/'
    last_inc = get_last_line(inc_path+'inc_log.txt')
    inc_filename = f"{inc_path}{last_inc}"
    inc = pd.read_csv(inc_filename)
    create_file(df_filename,list(inc.columns))
    df = pd.read_csv(df_filename)
    concat = pd.concat([df,inc],axis=0)
    concat.to_csv(df_filename, index=False)
    write_on_txt(inc_filename, log_filename)
def transform_dataframe()->pd.DataFrame:
    dataframe = pd.read_csv('/opt/airflow/files/final/coins.csv')
    df = dataframe.drop(['Unnamed: 0'], axis=1).reset_index(drop=True)
    df.rename(columns={
        "Código": "Codigo",
        'Var.':'Var',
        'Var.%':'Var%',},
        inplace=True 
    )
    df.to_csv('/opt/airflow/files/transformed/transformed.csv',index=False)
    return df.to_json()