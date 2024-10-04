import chardet
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Database credentials and settings
database_username = 'postgres'
database_password = 'PWS@120201a'
database_ip = '127.0.0.1'
database_name = 'dentajoy1'
csv_file_path = './mnt/data/lrfm_imed/rfm_proc.csv'  # Update this path
table_name = 'LEFM_imed'  # Update this table name

# Function to split data into chunks
def split_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Function to send data to the database
def send_to_db(data_chunk, table_name):
    encoded_password = quote_plus(database_password)
    database_url = f'postgresql://{database_username}:{encoded_password}@{database_ip}/{database_name}'
    engine = create_engine(database_url)
    data_chunk.to_sql(table_name, engine, if_exists='append', index=False)  # Change 'append' to 'replace' if needed

# Main function
def main():
    with open(csv_file_path, 'rb') as file:
        print('Detecting encoding...')
        result = chardet.detect(file.read())
        encoding = result['encoding']

    data = pd.read_csv(csv_file_path, encoding=encoding, low_memory=False)
    chunk_size = 10000  # Number of rows per chunk

    for chunk in split_data(data, chunk_size):
        print(f'Sending {chunk_size} rows to the database...')
        send_to_db(chunk, table_name)

if __name__ == '__main__':
    main()
