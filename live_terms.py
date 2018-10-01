import os
import pandas as pd
import datetime
from datetime import datetime, timedelta
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from data_tagging import *
from sqlalchemy import create_engine
from terms_data_to_google_spread import update_google_term_data

pd.options.mode.chained_assignment = None  # default='warn'

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

# read the up to date termination file

target_date = input('What is the target date? (mm_dd_yyyy) ')
ref_date = input('What is the date to match back to the population? (mm_dd_yyyy) ')
file = ('C:\\Users\\DuEvans\\Downloads\\terms_' + target_date + '.xlsx')
records_date = datetime.strptime(target_date, '%m_%d_%Y')
ref_file = ('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp\\ktp_all_pop_' + ref_date + '.csv')
new_terms = pd.read_excel(file, skiprows=5, encoding='latin1')
new_terms = new_terms.rename(columns={'Employee ID': 'ID'})

#new_terms = new_terms.loc[new_terms['Time Type'] == 'Full time']

# read the historic termination file
#old_terms = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\historic_terms.csv')
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\')
engine = create_engine('sqlite:///termination_records.db', echo=False)

old_terms = pd.read_sql_query("SELECT * FROM termination_records", con=engine)

# find just the new terminations
# merge old on new and see which ones don't match on employee ID
old_set = old_terms[['ID']]
old_set['Present'] = 'A'
# merge back to the dataframe and flag those where value was not present in both
find_new = pd.merge(new_terms, old_set, on=['ID'], how='left', indicator=True)
find_new['_merge'] == 'left_only'
new_terms = find_new.loc[find_new['_merge'] == 'left_only']
print('Length of new termination list: ' + str(len(new_terms)))
new_terms = new_terms.drop(['_merge', 'Present'], axis=1)
new_terms = new_terms.rename(columns={'Cost Center': 'Cost Centers'})


# format the new terminations to match the old

# import tagged information from population records
#  digital, structure, ethnicity, etc
df_demo = pd.read_csv(ref_file)
demographics = df_demo[['ID', 'Gender', 'Date of Birth (Locale Sensitive)', 'Ethnicity',
                        'Structure', 'Group', 'Team', 'Structure B', 'Age',
                        'Age Bracket', 'Management Level A', 'di_leader',
                        'di_poc', 'Prepare/New A', 'Prepare/New B', 'Digital A',
                        'Digital B', 'brm_key', 'Activity', 'Process', 'Category',
                        'hierarchy_lvl_1', 'hierarchy_lvl_2', 'hierarchy_lvl_3', 'hierarchy_lvl_1_id',
                        'hierarchy_lvl_2_id', 'hierarchy_lvl_3_id']]

# merge demographic information to the new terms
new_terms = pd.merge(new_terms, demographics, on=['ID'], how='left')

# set age at termination
new_terms['dob'] = pd.to_datetime(new_terms['Date of Birth (Locale Sensitive)'])
new_terms['days_old_at_term'] = (records_date - (new_terms['dob'])).dt.days
new_terms['yrs_old_at_term'] = (new_terms['days_old_at_term'] / 365).round(0)

# set tenure at termination
new_terms['doh'] = pd.to_datetime(new_terms['Hire Date'])
new_terms['days_tenure_at_term'] = (records_date - (new_terms['doh'])).dt.days
new_terms['yrs_tenure_at_term'] = (new_terms['days_tenure_at_term'] / 365).round(2)

def label_rifs(data):
    """identifies terminations that are strictly RIFs"""
    # find the RIFs
    rifs1 = data.loc[
        data['Primary Termination Reason'] == 'Terminate Employee > Involuntary > Elimination of Position']
    rifs2 = data.loc[
        data['Secondary Termination Reasons'] == 'Terminate Employee > Involuntary > Elimination of Position']
    rifs = rifs1.append(rifs2)

    rifs['is_rif'] = 'RIF'

    rifs_add = rifs[['ID', 'is_rif']]

    # add the rifs into the full new dataset
    data = pd.merge(data, rifs_add, on=['ID'], how='left')

    return data

new_terms = label_rifs(new_terms)


# append the terminations to the historic termination dataset
all_terms = old_terms.append(new_terms, sort=False)
all_terms.pop('Organization Assignments')
all_terms = all_terms.drop_duplicates()
# map the termination category field into something usable
all_terms['Term Type'] = all_terms['Termination Category'].map({'Terminate Employee > Voluntary': 'vol',
                                                                'Terminate Employee > Involuntary': 'invol'})

# save the updated termination spreadsheet

def update_historic_term_records(data):
    """
    Updates a database containing all termination records.
    :param data:
    :return:
    """
    os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\')
    # connect to the database
    engine = create_engine('sqlite:///termination_records.db', echo=False)

    # append new data to the database
    data.to_sql('termination_records', con=engine, if_exists='append', index=False)

    # test for duplicate values on 'dup_test' - JR number + employee ID
    df = pd.read_sql_query("SELECT * FROM termination_records", con=engine)
    df['dup_test'] = df['ID'] + df['Termination Date']
    df = df.drop_duplicates(subset=['dup_test'])
    df = df.drop(columns=['dup_test'])
    df.to_sql('termination_records', con=engine, if_exists='replace', index=False)
    print('Termination database updated.')

# prompt update to historic database
prompt_db_update = input('Update historic termination database? (y/n) ')
if prompt_db_update == 'y':
    update_historic_term_records(all_terms)
elif prompt_db_update == 'n':
    pass

#####################delete if not using in future#############################
# previous use - update csv records of terminations
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\')
all_terms.to_csv('historic_terms.csv', index=False)

# repeat as isolated to just full time
ft_flt = all_terms['Time Type'] == 'Full time'
ft_terms = all_terms[ft_flt]
ft_terms.drop_duplicates()
ft_terms.to_csv('historic_terms_ft.csv', index=False)
####################delete if not using in future################################


# prompt update of google spreadsheets
prompt_google = input('Update google spreadsheets with term data? (y/n) ')
if prompt_google == 'y':
    update_google_term_data(ft_terms, records_date)
elif prompt_google == 'n':
    pass

print('Termination updates complete.')
