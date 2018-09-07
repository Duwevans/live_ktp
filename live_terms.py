import os
import pandas as pd
import datetime
from datetime import datetime, timedelta
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from data_tagging import *

pd.options.mode.chained_assignment = None  # default='warn'


os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

# read the up to date termination file

target_date = input('\nWhat is the target date? (mm_dd_yyyy) ')

ref_date = input('\nWhat is the date to match back to the population? (mm_dd_yyyy) ')

file = ('C:\\Users\\DuEvans\\Downloads\\terms_' + target_date + '.xlsx')
records_date = datetime.strptime(target_date, '%m_%d_%Y')

ref_file = ('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + ref_date + '.csv')
#file = ('C:\\Users\\DuEvans\\Downloads\\terms_08_03_2018.xlsx')



new_terms = pd.read_excel(file, skiprows=5, encoding='latin1')

#new_terms = pd.read_excel('C:\\Users\\DuEvans\\Downloads\\terms_08_03_2018.xlsx', encoding='latin1')

new_terms = new_terms.rename(columns={'Employee ID': 'ID'})

new_terms = new_terms.loc[new_terms['Time Type'] == 'Full time']

# read the historic termination file

old_terms = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\historic_terms.csv')


# find just the new terminations

# merge old on new and see which ones don't match on employee ID
old_set = old_terms[['ID']]
old_set['Present'] = 'A'

# merge back to the dataframe and flag those where value was not present in both
find_new = pd.merge(new_terms, old_set, on=['ID'], how='left', indicator=True)
find_new['_merge'] == 'left_only'

new_terms = find_new.loc[find_new['_merge'] == 'left_only']
print('\nLength of new termination list: ')
print(len(new_terms))


new_terms = new_terms.drop(['_merge', 'Present'], axis=1)


# todo: format the new terminations to match the old

####################################################

#  get that good old manager key in here

new_terms = manager_map(new_terms)


# map against BRM categories
new_terms = new_terms.rename(columns={'Cost Center': 'Cost Centers'})
new_terms = brm_map(new_terms)
##################################################


# todo: import age, ethnicity, gender into the list of new terminations

df_demo = pd.read_csv(ref_file)
demographics = df_demo[['ID', 'Gender', 'Date of Birth (Locale Sensitive)', 'Race/Ethnicity (Locale Sensitive)']]

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


# map management levels
new_terms = management_levels(new_terms)
# map ethnicity
new_terms = map_ethnicity(new_terms)

# label everything as either 'Prepare' or 'New'
#   this is just either 'Prepare,' or 'New'
new_terms = prepare_or_new(new_terms)

# label everything into current digital/technology/marketing roles
new_terms = digital_map(new_terms)


# find the RIFs

rifs1 = new_terms.loc[new_terms['Primary Termination Reason'] == 'Terminate Employee > Involuntary > Elimination of Position']
rifs2 = new_terms.loc[new_terms['Secondary Termination Reasons'] == 'Terminate Employee > Involuntary > Elimination of Position']
rifs = rifs1.append(rifs2)

rifs['is_rif'] = 'RIF'

rifs_add = rifs[['ID', 'is_rif']]

# add the rifs into the full new dataset
new_terms = pd.merge(new_terms, rifs_add, on=['ID'], how='left')


# append the terminations to the historic termination dataset

all_terms = old_terms.append(new_terms, sort=False)

all_terms.pop('Organization Assignments')

all_terms = all_terms.drop_duplicates()

# map the termination category field into something usable
all_terms['Term Type'] = all_terms['Termination Category'].map({'Terminate Employee > Voluntary': 'vol',
                                                                'Terminate Employee > Involuntary': 'invol'})

# save the updated termination spreadsheet

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\')
all_terms.to_csv('historic_terms.csv', index=False)




# todo: run the termination prediction stuff


# isolate and save full time terms to a spreadsheet

ft_flt = all_terms['Time Type'] == 'Full time'
ft_terms = all_terms[ft_flt]
ft_terms.drop_duplicates()
ft_terms.to_csv('historic_terms_ft.csv', index=False)

# for sending to google sheets
terms0 = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\historic_terms_ft.csv', encoding='latin1')
terms0 = terms0.drop_duplicates(subset=['ID'])
terms0['Termination Date'] = pd.to_datetime(terms0['Termination Date'])
terms_prepare = terms0.loc[terms0['Prepare/New A'] == 'Prepare']

# map structure B and digital again because they started getting removed for some reason
terms_prepare['Structure B'] = terms_prepare['Group'].map({'Admissions Group': 'Admissions', 'Technology': 'Common',
                                                       'NXT': 'NXT', 'Licensure Group': 'Licensure',
                                                       'Med': 'Licensure', 'Finance & Accounting': 'Common',
                                                       'Admissions Faculty': 'Admissions', 'Nursing': 'Licensure',
                                                       'MPrep': 'Admissions', 'Marketing': 'Common', 'Bar': 'Licensure',
                                                       'HR / PR / Admin': 'Common', 'Publishing': 'Common',
                                                       'Data and Learning Science': 'Common', 'Metis': 'New Ventures',
                                                       'Digital Media': 'Common', 'iHuman': 'New Ventures',
                                                       'Advise': 'Advise', 'International': 'Licensure',
                                                       'Metis Faculty': 'New Ventures', 'Admissions Core': 'Admissions',
                                                       'Admissions New': 'Admissions', 'Allied Health': 'Licensure',
                                                       'Legal': 'Common', 'TTL Labs': 'New Ventures', 'Executive': 'Common',
                                                       'Licensure Programs': 'Licensure'})

terms_prepare = digital_map(terms_prepare)

# set age at termination
terms_prepare['dob'] = pd.to_datetime(terms_prepare['Date of Birth (Locale Sensitive)'])

terms_prepare['days_old_at_term'] = (records_date - (terms_prepare['dob'])).dt.days

terms_prepare['yrs_old_at_term'] = (terms_prepare['days_old_at_term'] / 365).round(0)

# set tenure at termination
terms_prepare['doh'] = pd.to_datetime(terms_prepare['Hire Date'])
terms_prepare['days_tenure_at_term'] = (records_date - (terms_prepare['doh'])).dt.days
terms_prepare['yrs_tenure_at_term'] = (terms_prepare['days_tenure_at_term'] / 365).round(2)

# create a separate frame of the rolling 12 month headcount metrics

records_date = pd.to_datetime(records_date)
roll_12_date = records_date - timedelta(days=365)

terms_roll_12 = terms_prepare.loc[terms_prepare['Termination Date'] > roll_12_date]

# create a separate frame for 2017
terms1 = terms_prepare.loc[terms_prepare['Termination Date'] > '12/31/2016']
terms2017 = terms1.loc[terms1['Termination Date'] < '01/01/2018']

# create a separate frame for 2018
terms2018 = terms_prepare.loc[terms_prepare['Termination Date'] > '12/31/2017']


# create a non-confidential termination dataset for all the same cuts
noncf_columns = ['Gender', 'Race/Ethnicity (Locale Sensitive)', 'Date of Birth (Locale Sensitive)',
                 'Total Pay - Amount', 'dob', 'days_old_at_term', 'yrs_old_at_term',
                 'Ethnicity']

# create datasets just for the HR team dashboard (no comp)
hr_team_cols = ['Total Pay - Amount']
hr_terms_2018 = terms2018.drop(columns=hr_team_cols)
hr_terms_rolling = terms_roll_12.drop(columns=hr_team_cols)

all_terms = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\historic_terms.csv', encoding='latin1')
hr_team_all = all_terms.drop(columns=hr_team_cols)

terms2017_noncf = terms2017.drop(columns=noncf_columns)
terms2018_noncf = terms2018.drop(columns=noncf_columns)
terms_roll_12_noncf = terms_roll_12.drop(columns=noncf_columns)

# send all cuts of terminations to a confidential spreadsheet

from oauth2client.service_account import ServiceAccountCredentials

print('\nPinging our Google overlords...')

os.chdir('C:\\USers\\DuEvans\\Documents\\ktp_data')
# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# hr team dashboard
hr_team_dashboard = client.open('terms_2018').sheet1
hr_team_dashboard.clear()
gspread_dataframe.set_with_dataframe(hr_team_dashboard, hr_terms_2018)

hr_team_dashboard = client.open('terms_rolling').sheet1
hr_team_dashboard.clear()
gspread_dataframe.set_with_dataframe(hr_team_dashboard, hr_terms_rolling)

hr_team_dashboard = client.open('all_terms').sheet1
hr_team_dashboard.clear()
gspread_dataframe.set_with_dataframe(hr_team_dashboard, hr_team_all)
print('\nHR team dashboard updated.')

# non_confidential dashbaord
noncf_2017_sheet = client.open('Prepare Turnover Dashboard v1.0').worksheet('2017 Data')
noncf_2017_sheet.clear()
gspread_dataframe.set_with_dataframe(noncf_2017_sheet, terms2017_noncf)

noncf_2018_sheet = client.open('Prepare Turnover Dashboard v1.0').worksheet('2018 Data')
noncf_2018_sheet.clear()
gspread_dataframe.set_with_dataframe(noncf_2018_sheet, terms2018_noncf)

noncf_rolling_sheet = client.open('Prepare Turnover Dashboard v1.0').worksheet('Rolling 12 Data')
noncf_rolling_sheet.clear()
gspread_dataframe.set_with_dataframe(noncf_rolling_sheet, terms_roll_12_noncf)
print('\nER team dashboard updated.')



# todo: D&I (confidential) dashboard
cf_2017_sheet = client.open('D&I Progress v1.1').worksheet('2017 terms')
cf_2017_sheet.clear()
gspread_dataframe.set_with_dataframe(cf_2017_sheet, terms2017)

cf_2018_sheet = client.open('D&I Progress v1.1').worksheet('2018 terms')
cf_2018_sheet.clear()
gspread_dataframe.set_with_dataframe(cf_2018_sheet, terms2018)

cf_rolling_sheet = client.open('D&I Progress v1.1').worksheet('rolling terms')
cf_rolling_sheet.clear()
gspread_dataframe.set_with_dataframe(cf_rolling_sheet, terms_roll_12)
print('\nD&I Dashboard updated.')





# write each of the sheets with new data
#gspread_dataframe.set_with_dataframe(sheet, ft_terms)

# get today's date, again, but format it as mm/dd/yyyy, and include the time
dt = datetime.now()
dt_pretty = dt.strftime("%m/%d/%y %I:%M%p")

# format into a string
last_update = 'Data updated at: ' + dt_pretty + '.'
print('\n')
print(last_update)

print('\Process finished.')
