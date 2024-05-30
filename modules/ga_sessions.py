import logging
import os
import json
import pandas as pd

path = os.environ.get('PROJECT_PATH', '..')


def add_to_(df: pd.DataFrame) -> pd.DataFrame:
    # get list of .json files with ga_sessions
    files_list = os.listdir(f'{path}/data/ga_sessions_new/')
    # read files in data and adding to DataFrame
    for file_name in files_list:
        with open(f'{path}/data/ga_sessions_new/{file_name}') as fin:
            data_new = json.load(fin)
            # date of data
            sessions_date = file_name.split('.')[0].split('_')[-1]
            data = pd.DataFrame.from_dict(data_new[sessions_date])
            # add a new data to the table
            df = pd.concat([data, df], axis=0)
            logging.info(f'{df.shape} - размер датафрейма после добавления данных от {sessions_date}')
    return df


def add_ga_sessions() -> None:
    df = pd.read_csv(f'{path}/data/ga_sessions.csv', low_memory=False)
    logging.info(f'{df.shape} - исходный размер данных ga_sessions.csv')
    add_to_(df)
    df.to_csv(f'{path}/data/ga_sessions_new.csv', sep=',', index=False)
    logging.info('Новые данные внесены в таблицу ga_sessions_new.csv')


if __name__ == '__main__':
    add_ga_sessions()
