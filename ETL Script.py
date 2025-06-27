import pymysql
import os
import pandas as pd
from datetime import datetime

def fetch_data_mysql():
    connection = pymysql.connect(
            host="localhost",
            user='root',
            password='root',
            database='test',
    )

    query = 'SELECT * FROM sample_data'
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def transform_data(df):
    df_transformed = df[df['age'] > 30]
    return df_transformed

def write_data_to_file(df):
    outputDir = '/home/ubuntu/extract'
    os.makedirs(outputDir,exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"etlOutput_{timestamp}.csv"
    filePath = os.path.join(outputDir,filename)
    df.to_csv(filePath, index=False)
    print(f"Data written to {filePath}")

def etl_process():
    df = fetch_data_mysql()
    df_transformed = transform_data(df)
    write_data_to_file(df_transformed)

if __name__ == "__main__":
    etl_process()

