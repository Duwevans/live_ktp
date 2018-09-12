import pandas as pd
import os
from sqlalchemy import create_engine
import time


def update_population_records(ft_df, all_df):
    """
    appends historic population files with new data
    inclues ft file and all population file
    :param data:
    :return:
    """

    start_time_1 = time.time()

    file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\'
    os.chdir(file_path)

    # connect to the database
    all_engine = create_engine('sqlite:///all_ktp_db', echo=False)

    # append new data to the database
    all_df.to_sql('all_ktp', con=all_engine, if_exists='append', index=False)

    # test for number of entries per 'record_date'
    df_all = pd.read_sql_query("SELECT * FROM all_ktp", con=all_engine)
    df_all = df_all.drop_duplicates()
    df_all.to_sql('all_ktp', con=all_engine, if_exists='replace', index=False)

    lap_time_1 = time.time() - start_time_1

    # repeat for the full time dataset
    ft_engine = create_engine('sqlite:///ft_ktp_db', echo=False)
    ft_df.to_sql('ft_ktp', con=ft_engine, if_exists='append', index=False)
    df_ft_all = pd.read_sql_query("SELECT * FROM ft_ktp", con=ft_engine)
    df_ft_all['record_date'].value_counts()

    # check for duplicate values
    df_ft_all = df_ft_all.drop_duplicates()
    # save without duplicate values
    df_ft_all.to_sql('ft_ktp', con=ft_engine, if_exists='replace', index=False)

    print('\nTime to update population databases: ' + time.strftime("%H:%M:%S", time.gmtime(lap_time_1)))

