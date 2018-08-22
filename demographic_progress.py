import pandas as pd
import os
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import shutil
import os
from datetime import datetime
from datetime import date
from sys import exit

pd.options.mode.chained_assignment = None  # default='warn'


# hold this as the starting test file
#pop_df = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_08_13_2018.csv', encoding='latin1')

# read the starting file
file_date = input('\nWhat is the most recent target date? (mm_dd_yyyy) ')

pop_file = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + file_date + '.csv'
#ref_file =

pop_df = pd.read_csv(pop_file, encoding='latin1')
#df_ref = pd.read_csv(ref_file, encoding='latin1')



# todo: calculate the gender diversity index for each OU, Group, and Team
# pop is the ft and formatted data set

print('\nCalculating diversity and gender indices...')





# todo: calculate the ethnicity diversity index for each OU, Group, and Team

pivots = ['Structure', 'Group', 'Team', 'Activity', 'Process', 'Category', 'Digital']

def calc_diversity_indices(dataset, pivots):
    """Calculates gender, ethnicity, and age diversity indices for each of the pivot categories provided
    Appends historic dataframe with new info
    Writes most recent information to google spreadsheet."""
    #df0 = pd.read_csv(historic_file)

    df1 = pd.DataFrame()

    # list of ethnicities possible
    ethnicities = ['american_indian', 'asian', 'black', 'hispanic', 'pacific_islander', 'two_or_more', 'white']
    # list of ages possible
    ages = ['25 to 34', '35 to 44', '45 to 54', '55 to 64', '65 +', '18 to 25']
    # list of gender possible
    genders = ['female', 'male']

    # dataset = pop_df
    # pivots = ['Digital', 'Category', 'Team', 'Group']
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
                                                    "('ID', '65+')": '65 +', "('ID', '<25')": '18 to 25'})

        # make sure all possible age brackets appear in the dataset
        pivoted_ages = (list(df_age_pivot.columns.values))
        for age in ages:
            if age not in pivoted_ages:
                df_eth_pivot[age] = 0

        df_age_pivot['age_total'] = (df_age_pivot['25 to 34'] + df_age_pivot['35 to 44'] + df_age_pivot['45 to 54']
                                    + df_age_pivot['55 to 64'] + df_age_pivot['65 +'] + df_age_pivot['18 to 25'])
        df_age_pivot['under_35_total'] = (df_age_pivot['25 to 34'] + df_age_pivot['18 to 25'])
        df_age_pivot['pct_under_35'] = df_age_pivot['under_35_total'] / df_age_pivot['age_total']

        # merge all three dataframes into one

        df_pivot = pd.merge(df_eth_pivot, df_gdr_pivot, on=pivot, how='left')
        df_pivot = df_pivot.drop_duplicates(subset=pivot)
        df_pivot = pd.merge(df_pivot, df_age_pivot, on=pivot, how='left')
        df_pivot = df_pivot.drop_duplicates(subset=pivot)

        # todo: create separate dataframe for each pivot

        df_name = 'df_' + str(pivot)

        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\di_indices')

        df_pivot.to_csv(df_name + '.csv', index=False)

        df_pivot = df_pivot.rename(columns={pivot: 'set'})
        df_pivot['slice'] = str(pivot)

        # append df_pivot into broader dataframe
        df1 = df1.append(df_pivot, sort=False)

    # write the D&I indices to a google sheet as backend

    update_sheets = input('\nUpdate google spreadsheets? (y/n) ')
    if update_sheets == 'y':
        pass
    elif update_sheets == 'n':
        print('\nProcess finished.')
        exit()

    # use creds to create a client to interact with the Google Drive API
    os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # find each datasheet, clear the current values, write new data
    demographic_sheet = client.open("KTP Demographic Dashboard v1.3").worksheet('progress_data')
    demographic_sheet.clear()
    gspread_dataframe.set_with_dataframe(demographic_sheet, df1)

    print('\nDemographic progress dashboard updated - KTP Demographic Dashboard v1.3')

    return df1

calc_diversity_indices(pop_df, pivots)



# todo: find the total number of employees reporting to women and non-white individuals

today = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_08_13_2018.csv')
meid = today[['Manager ID']]
meid = today[['ID','Manager ID']]
meid = today[['Manager ID']]
meid = meid.drop_duplicates(subset=['Manager ID'])
meid = meid.rename(columns={'Manager ID': 'ID'})
gend_eth = today[['ID', 'Gender', 'Ethnicity']]
meid = pd.merge(meid, gend_eth, on=['ID'], how='left')
list(meid)

meid = meid.rename(columns={'ID': 'Manager ID', 'Gender': 'mgr_gdr', 'Ethnicity': 'mgr_eth'})
today = pd.merge(today, meid, on='Manager ID', how='left')

today['mgr_eth'].value_counts()

today['mgr_eth_binary'] = today['mgr_eth'].map({'Asian': 'Person of color', 'Hispanic': 'Person of color', 'Black': 'Person of color', 'Two or More': 'Person of color', 'White': 'White'})
mgr_gdr_report = pd.pivot_table(today, values=['ID'], index=['mgr_gdr'], aggfunc=len)
mgr_eth_report = pd.pivot_table(today, values=['ID'], index=['mgr_eth_binary'], aggfunc=len)

yesterday = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_01_01_2018.csv', encoding='latin1')
ymeid = yesterday[['Manager ID']]
ymeid = yesterday[['ID','Manager ID']]
ymeid = yesterday[['Manager ID']]
ymeid = ymeid.drop_duplicates(subset=['Manager ID'])
ymeid = ymeid.rename(columns={'Manager ID': 'ID'})
gend_eth = yesterday[['ID', 'Gender', 'Ethnicity']]
ymeid = pd.merge(ymeid, gend_eth, on=['ID'], how='left')
list(ymeid)

ymeid = ymeid.rename(columns={'ID': 'Manager ID', 'Gender': 'mgr_gdr', 'Ethnicity': 'mgr_eth'})
yesterday = pd.merge(yesterday, ymeid, on='Manager ID', how='left')

list(yesterday)
yesterday['mgr_eth'].value_counts()

yesterday['mgr_eth_binary'] = yesterday['mgr_eth'].map({'Asian': 'Person of color', 'Hispanic': 'Person of color', 'Black': 'Person of color', 'Two or More': 'Person of color', 'White': 'White'})
mgr_gdr_report = pd.pivot_table(yesterday, values=['ID'], index=['mgr_gdr'], aggfunc=len)
mgr_eth_report = pd.pivot_table(yesterday, values=['ID'], index=['mgr_eth_binary'], aggfunc=len)

mgr_gdr_report
mgr_eth_report

