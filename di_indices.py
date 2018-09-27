import pandas as pd
from datetime import datetime
from datetime import date
import os
from sqlalchemy import create_engine
import time
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

pd.options.mode.chained_assignment = None  # default='warn'


def calc_diversity_indices(target_date, dataset):
    """
    Calculates gender, ethnicity, and age diversity indices for each of the pivot categories provided
    Appends historic dataframe with new info
    """
    records_date = datetime.strptime(target_date, '%m_%d_%Y')

    pivots = ['Structure B', 'Group', 'Team', 'Activity', 'Process', 'Category', 'Digital A', 'Digital B',
              'Management Level A']

    df1 = pd.DataFrame()

    # list of ethnicities possible
    ethnicities = ['american_indian', 'asian', 'black', 'hispanic', 'pacific_islander', 'two_or_more', 'white']
    # list of ages possible
    ages = ['25 to 34', '35 to 44', '45 to 54', '55 to 64', '65 +', '18 to 24']
    # list of gender possible
    genders = ['female', 'male']

    for pivot in pivots:
        #df_name =  'df_' + str(pivot).lower()
        # calculate % people of color for the pivot group
        df_eth_pivot = pd.pivot_table(dataset, values=['ID'], index=[pivot], columns=['Ethnicity'], aggfunc=len)
        df_eth_pivot = pd.DataFrame(df_eth_pivot.to_records())
        df_eth_pivot = df_eth_pivot.fillna(0)
        df_eth_pivot = df_eth_pivot.rename(columns={"('ID', 'American Indian')": 'american_indian', "('ID', 'Asian')": 'asian',
                                        "('ID', 'Black')": 'black', "('ID', 'Hispanic')": 'hispanic',
                                        "('ID', 'Pacific Islander')": 'pacific_islander', "('ID', 'Two or more')": 'two_or_more',
                                        "('ID', 'White')": 'white', "('ID', 'dni')": 'dni'})

        # make sure all possible ethnicity appear in the dataset
        pivoted_ethnicities = (list(df_eth_pivot.columns.values))
        for ethnicity in ethnicities:
            if ethnicity not in pivoted_ethnicities:
                df_eth_pivot[ethnicity] = 0


        df_eth_pivot['eth_total'] = (df_eth_pivot['american_indian'] + df_eth_pivot['asian'] + df_eth_pivot['black'] + df_eth_pivot['hispanic']\
                      + df_eth_pivot['pacific_islander'] + df_eth_pivot['two_or_more'] + df_eth_pivot['white'])

        df_eth_pivot['poc'] = (df_eth_pivot['american_indian'] + df_eth_pivot['asian'] + df_eth_pivot['black']
                        + df_eth_pivot['hispanic'] + df_eth_pivot['pacific_islander'] + df_eth_pivot['two_or_more'])

        df_eth_pivot['pct_poc'] = df_eth_pivot['poc'] / df_eth_pivot['eth_total']

        # calculate % female for the pivot group
        df_gdr_pivot = pd.pivot_table(dataset, values=['ID'], index=[pivot], columns=['Gender'], aggfunc=len)
        df_gdr_pivot = pd.DataFrame(df_gdr_pivot.to_records())
        df_gdr_pivot = df_gdr_pivot.fillna(0)
        df_gdr_pivot = df_gdr_pivot.rename(columns={"('ID', 'Female')": 'female', "('ID', 'Male')": 'male'})

        # make sure all possible genders appear in the dataset
        pivoted_genders = (list(df_gdr_pivot.columns.values))
        for gender in genders:
            if gender not in pivoted_genders:
                df_eth_pivot[gender] = 0

        df_gdr_pivot['gdr_total'] = df_gdr_pivot['male'] + df_gdr_pivot['female']
        df_gdr_pivot['pct_female'] = df_gdr_pivot['female'] / df_gdr_pivot['gdr_total']

        # calculate the % under 35 for the pivot group
        df_age_pivot = pd.pivot_table(dataset, values=['ID'], index=[pivot], columns=['Age Bracket'], aggfunc=len)
        df_age_pivot = pd.DataFrame(df_age_pivot.to_records())
        df_age_pivot = df_age_pivot.fillna(0)
        df_age_pivot = df_age_pivot.rename(columns={"('ID', '25 to 34')": '25 to 34', "('ID', '35 to 44')": '35 to 44',
                                                    "('ID', '45 to 54')": '45 to 54', "('ID', '55 to 64')": '55 to 64',
                                                    "('ID', '65+')": '65 +', "('ID', '18 to 24')": '18 to 24'})

        # make sure all possible age brackets appear in the dataset
        pivoted_ages = (list(df_age_pivot.columns.values))
        for age in ages:
            if age not in pivoted_ages:
                df_eth_pivot[age] = 0

        df_age_pivot['age_total'] = (df_age_pivot['25 to 34'] + df_age_pivot['35 to 44'] + df_age_pivot['45 to 54']
                                    + df_age_pivot['55 to 64'] + df_age_pivot['65 +'] + df_age_pivot['18 to 24'])
        df_age_pivot['under_35_total'] = (df_age_pivot['25 to 34'] + df_age_pivot['18 to 24'])
        df_age_pivot['pct_under_35'] = df_age_pivot['under_35_total'] / df_age_pivot['age_total']

        # merge all three dataframes into one

        df_pivot = pd.merge(df_eth_pivot, df_gdr_pivot, on=pivot, how='left')
        df_pivot = df_pivot.drop_duplicates(subset=pivot)
        df_pivot = pd.merge(df_pivot, df_age_pivot, on=pivot, how='left')
        df_pivot = df_pivot.drop_duplicates(subset=pivot)

        # create separate dataframe for each pivot
        df_name = 'df_' + str(pivot)

        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\di_indices')

        df_pivot.to_csv(df_name + '.csv', index=False)

        df_pivot = df_pivot.rename(columns={pivot: 'set'})
        df_pivot['slice'] = str(pivot)

        # append df_pivot into broader dataframe
        df1 = df1.append(df_pivot, sort=False)

        df1['record_date'] = records_date
        df1['record_date'] = pd.to_datetime(df1['record_date'])


    def update_di_indices_database(data):
        """
        Reads and updates the records database for D&I indices
        :param data:
        :return:
        """
        start_time_1 = time.time()
        # orient to the directory
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\di_indices')

        # connect to the database
        engine = create_engine('sqlite:///di_indices_db', echo=False)

        # append new data to the database
        data.to_sql('di_indices', con=engine, if_exists='append', index=False)

        # test for number of entries per 'record_date'
        df = pd.read_sql_query("SELECT * FROM di_indices", con=engine)
        df = df.drop_duplicates()
        df.to_sql('di_indices', con=engine, if_exists='replace', index=False)

        lap_time_1 = time.time() - start_time_1

        print('Time to D&I indices population databases: ' + time.strftime("%H:%M:%S", time.gmtime(lap_time_1)))


    def update_google_spread(data):
        """
        Updates any related google spreadsheets with new data
        :param data:
        :return:
        """
        # use creds to create a client to interact with the Google Drive API
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
        client = gspread.authorize(creds)

        # find each datasheet, clear the current values, write most recent data
        #demographic_sheet = client.open("D&I Progress v1.0").worksheet('group_pct_data')
        #demographic_sheet.clear()
        #gspread_dataframe.set_with_dataframe(demographic_sheet, df1)

        # find datasheet, write historic data
        # grab historic data
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\di_indices')

        # connect to the database
        engine = create_engine('sqlite:///di_indices_db', echo=False)

        # read the database
        records = pd.read_sql_query("SELECT * FROM di_indices", con=engine)

        # write to google spreadsheet
        gs1 = client.open('all_di_indices_records').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, records)

        print('Google spreadsheets updated.')

    # prompt user input to update the D&I indices database
    prompt_db_update = input('Update D&I indices database? (y/n) ')
    if prompt_db_update == 'y':
        update_di_indices_database(df1)
    elif prompt_db_update == 'n':
        pass

    # prompt user input to update any related google spreadsheets
    prompt_google_update = input('Update D&I indices google spreadsheets? (y/n) ')
    if prompt_google_update == 'y':
        update_google_spread(df1)
    elif prompt_google_update == 'n':
        pass

    print('D&I indices process complete.')
