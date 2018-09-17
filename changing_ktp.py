import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import os
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sys import exit
from data_tagging import *


pd.options.mode.chained_assignment = None  # default='warn'

file_date_a = input('\nDate today? (mm_dd_yyyy) ')
file_date_b = input('\nDate yesterday? (mm_dd_yyyy) ')

file_a = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp\\ktp_all_pop_' + file_date_a + '.csv'
file_b = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp\\ktp_all_pop_' + file_date_b + '.csv'


pop_1 = pd.read_csv(file_a, encoding='latin1')
pop_0 = pd.read_csv(file_b, encoding='latin1')

date_a = datetime.strptime(file_date_a, '%m_%d_%Y')

date_b = datetime.strptime(file_date_b, '%m_%d_%Y')

# create numbers for management levels
pop_1['mgt_lvl'] = pop_1['Management Level'].map({'11 Individual Contributor': 0,
                                                          '10 Supervisor': 1,
                                                          '9 Manager ': 1,
                                                          '8 Senior Manager': 1,
                                                          '7 Director': 2,
                                                          '6 Exec & Sr. Director/Dean': 3,
                                                          '5 VP': 4,
                                                          '4 Senior VP': 5,
                                                          '3 Executive VP': 6,
                                                          '2 Senior Officer': 7})

pop_0['mgt_lvl'] = pop_0['Management Level'].map({'11 Individual Contributor': 0,
                                                          '10 Supervisor': 1,
                                                          '9 Manager ': 1,
                                                          '8 Senior Manager': 1,
                                                          '7 Director': 2,
                                                          '6 Exec & Sr. Director/Dean': 3,
                                                          '5 VP': 4,
                                                          '4 Senior VP': 5,
                                                          '3 Executive VP': 6,
                                                          '2 Senior Officer': 7})

# 'a' = after # 'b' = before

pop_1 = pop_1.set_index('ID')
pop_1 = pop_1.add_suffix('_a')

pop_0 = pop_0.set_index('ID')
pop_0 = pop_0.add_suffix('_b')

pop_all = pop_1.join(pop_0, on='ID', how='left', sort=False)

# find those that do not appear in the new list and append
pop_missing = pd.merge(pop_0, pop_1, on=['ID'], how='left', sort=False)
termed = pop_missing.loc[pop_missing['Manager ID_a'].isnull()]
pop_all = pop_all.append(termed, sort=False)

pop_all['ID'] = pop_all.index


os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\')

# below is temporary fix - only those who appear on both records

# identify change/no change in the four target fields

pop_all['time_a'] = pop_all['FT / PT_a'].map({'Full time': 1, 'Part time': 0})
pop_all['time_b'] = pop_all['FT / PT_b'].map({'Full time': 1, 'Part time': 0})

# rename to make the columns usable
pop_all = pop_all.rename(columns={'Total Base Pay Annualized - Amount_a': 'base_a',
                        'Total Base Pay Annualized - Amount_b': 'base_b',
                        'Job Profile (Primary)_a': 'job_a',
                        'Job Profile (Primary)_b': 'job_b',
                        'Scheduled Weekly Hours_a': 'hours_a',
                        'Scheduled Weekly Hours_b': 'hours_b'})


# calculate tenure
pop_all['(Most Recent) Hire Date_a'] = pd.to_datetime(pop_all['(Most Recent) Hire Date_a'])
pop_all['(Most Recent) Hire Date_b'] = pd.to_datetime(pop_all['(Most Recent) Hire Date_b'])

pop_all['days_tenure_a'] = (date_a - (pop_all['(Most Recent) Hire Date_a'])).dt.days
pop_all['yrs_tenure_a'] = (pop_all['days_tenure_a']/365).round(0)
pop_all['months_tenure_a'] = (pop_all['yrs_tenure_a']/12).round(0)

pop_all['days_tenure_b'] = (date_b - (pop_all['(Most Recent) Hire Date_b'])).dt.days
pop_all['yrs_tenure_b'] = (pop_all['days_tenure_b']/365).round(0)
pop_all['months_tenure_b'] = (pop_all['yrs_tenure_b']/12).round(0)

# find change in time type
pop_all['time_chg'] = pop_all['time_a'] - pop_all['time_b']

# find change in base pay
pop_all['base_chg'] = pop_all['base_a'] - pop_all['base_b']

# find change in weekly hours
pop_all['hours_chg'] = pop_all['hours_a'] - pop_all['hours_b']

# find change in management level
pop_all['mgt_lvl_chg'] = pop_all['mgt_lvl_a'] - pop_all['mgt_lvl_b']

pop_all = pop_all.sort_values(by=['base_chg'], ascending=False)


# Backing into to terminations / hires
# note - no manager ID B means this was a hire
# note - no manager ID A means this was a term

# create custom hire/term columns
pop_all['hire'] = np.where(pop_all['Manager ID_b'].isnull(), 1, 0)

pop_all['term'] = np.where(pop_all['Manager ID_a'].isnull(), 1, 0)

# fill all nan values on change fields with 0
columns = ['hire', 'term', 'mgt_lvl_chg', 'hours_chg', 'base_chg', 'time_chg']
dni_value = 0
for column in columns:
    pop_all[column] = pop_all[column].fillna(value=dni_value)

# save all records
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\all_records\\')
filename = ('ktp_pop_changes_' + file_date_a + '.csv')
pop_all = pop_all.drop_duplicates()
pop_all.to_csv(filename, index=False)
print('\nFull population with all changes saved.')

# isolate just the rows with changes
#   the fields that indicate changes are hire, term, mgt_lvl_chg, hours_chg, base_chg, time_chg
#   the dataframe to hold all changes will be called df_changes

print('\nNumber of weekly changes in...')
# add hires
hires = pop_all.loc[pop_all['hire'] == 1]
print('\nTotal hires: ' + str(len(hires)))

df_changes = hires

# add terms
terms = pop_all.loc[pop_all['term'] == 1]
print('\nTotal terminations: ' + str(len(terms)))

df_changes = hires.append(terms, sort=False)

# add changes to management level
mgt_lvl_chgs = pop_all.loc[pop_all['mgt_lvl_chg'] != 0]
df_changes = df_changes.append(mgt_lvl_chgs, sort=False)
print('\nManagement level: ' + str(len(mgt_lvl_chgs)))

# add changes to hours
hours_chgs = pop_all.loc[pop_all['hours_chg'] != 0]
df_changes = df_changes.append(hours_chgs, sort=False)
print('\nWeekly scheduled hours: ' + str(len(hours_chgs)))

# add changes to base compensation
base_chgs = pop_all.loc[pop_all['base_chg'] != 0]
df_changes = df_changes.append(base_chgs, sort=False)
print('\nBase compensation: ' + str(len(base_chgs)))

# add changes to time type
time_chgs = pop_all.loc[pop_all['time_chg'] != 0]
df_changes = df_changes.append(time_chgs, sort=False)
print('\nTime type: ' + str(len(time_chgs)))

# remove duplicate entries
print('\nTotal changes: ' + str(len(df_changes)))
df_changes.index.rename('ID')
df_changes = df_changes[~df_changes.index.duplicated(keep='first')]
print('\nUnique individuals with changes: ' + str(len(df_changes)))

# index the change dataframe by date
date = pd.to_datetime(date_a)
df_changes['change_date'] = date


# save the new changes to a csv file
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\daily_movement\\')
filename = 'ktp_population_changes_as_of_' + file_date_a + '.csv'
df_changes.to_csv(filename, index=False)


# get the previous file,
#  and save an archived version of the previous file

old_changes = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\ktp_population_movement_all_records.csv')
old_changes['change_date'] = pd.to_datetime(old_changes['change_date'])


# append the existing change dataframe with new data
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\')
updated_df = old_changes.append(df_changes, sort=False)

# these fields have to be converted back to datetime to ensure duplicate check passes
updated_df['(Most Recent) Hire Date_a'] = pd.to_datetime(updated_df['(Most Recent) Hire Date_a'])
updated_df['(Most Recent) Hire Date_b'] = pd.to_datetime(updated_df['(Most Recent) Hire Date_b'])

updated_df = updated_df.drop_duplicates()
updated_df.to_csv('ktp_population_movement_all_records.csv', index=False)


# update the full set to a google sheets backend
update_sheets = input('\nUpdate population changes google spreadsheets? (y/n) ')
if update_sheets == 'y':

    sheets_data = updated_df


    print('\nHear me, oh Great Google overseers...')

    os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\')
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # find the dashboard data sheet and clear the current values
    data_sheet = client.open("changing_ktp").sheet1
    data_sheet.clear()

    # write each of the sheets with new data
    gspread_dataframe.set_with_dataframe(data_sheet, sheets_data)

    print('\nGoogle sheets dashboard updated.')
elif update_sheets == 'n':
    pass


print('\nProcess finished.')
