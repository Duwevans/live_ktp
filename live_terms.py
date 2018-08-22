import os
import pandas as pd
import datetime
from datetime import datetime, timedelta
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

# read the up to date termination file

target_date = input('\nWhat is the target date? (mm_dd_yyyy) ')

file = ('C:\\Users\\DuEvans\\Downloads\\terms_' + target_date + '.xlsx')
records_date = datetime.strptime(target_date, '%m_%d_%Y')

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
find_new['_merge'] =='left_only'

new_terms = find_new.loc[find_new['_merge'] =='left_only']
print('\nLength of new termination list: ')
print(len(new_terms))


new_terms = new_terms.drop(['_merge', 'Present'], axis=1)


# todo: format the new terminations to match the old

####################################################

#  get that good old manager key in here

# read the manager key
mgr_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\meid_key.csv')
# read the eid key
eeid_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\eid_key.csv')
# format the eid key to match the manager map
eeid_key.columns = ['Name', 'ID', 'Structure', 'Group', 'Team']
new_terms = pd.merge(new_terms, mgr_key, on='Manager ID', how='left', sort=False)


# map against BRM categories
# create the needed field to map to BRM
new_terms['_1'] = new_terms['Cost Center'].str[:6]
new_terms['_2'] = new_terms['_1'].str[:2]
new_terms['_3'] = new_terms['_1'].str[-4:]
new_terms['_4'] = (new_terms['_2'] + "_" + new_terms['_3'])
new_terms['brm_key'] = (new_terms['Single Job Family'] + "_" + new_terms['_4'])
new_terms['brm_key'] = new_terms['brm_key'].str.lower()
new_terms = new_terms.drop(columns=['_1', '_2', '_3', '_4'])
# read the amend the BRM file
brm_map = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\brm_map.csv', encoding='latin1')
brm_map['brm_key'] = brm_map['LOCAL_ACTIVITY_ID']
brm_map['brm_key'] = brm_map['brm_key'].str.lower()
brm_map = brm_map[['brm_key', 'Activity', 'Process', 'Category']]

# merge the two files w/ BRM categories
new_terms = pd.merge(new_terms, brm_map, on='brm_key', how='left', sort=False)

# remove nan values from the matched dataset
mgr_nan = new_terms[new_terms['Structure'].isnull()]
# remove the nan values from the original dataset
mgr_mapped = new_terms[new_terms['Structure'].notnull()]
# drop the nan columns
mgr_nan = mgr_nan.drop(['Primary Key','Structure', 'Group', 'Team'], axis=1)
# match the values to the EEID map
eeid_mapped = pd.merge(mgr_nan, eeid_key, on='ID', how='left')
# compile the new dataset
new_terms = mgr_mapped.append(eeid_mapped, sort=False)
new_terms = new_terms.rename(columns={'Primary Key': 'Manager'})

##################################################


# todo: import age, ethnicity, gender into the list of new terminations
# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# find each datasheet and clear the current values
records_sheet = client.open("unedited records").worksheet('data')
df_demo = pd.DataFrame(records_sheet.get_all_records())
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


# map management levels
new_terms['Management Level1'] = new_terms['Management Level'].map({'11 Individual Contributor': 'Individual Contributor',
                                                        '9 Manager': 'Manager', '8 Senior Manager': 'Manager',
                                                        '10 Supervisor': 'Manager', '7 Director': 'Director',
                                                        '6 Exec & Sr. Director/Dean': 'Executive Director',
                                                        '5 VP': 'VP', '4 Senior VP': 'Above VP',
                                                        '2 Senior Officer': 'Above VP',
                                                        '3 Executive VP': 'Above VP'})
# map ethnicity
new_terms['Ethnicity'] = new_terms['Race/Ethnicity (Locale Sensitive)'].map({'White (Not Hispanic or Latino) (United States of America)': 'White',
                                    'Asian (Not Hispanic or Latino) (United States of America)': 'Asian',
                                    'Black or African American (Not Hispanic or Latino) (United States of America)': 'Black',
                                    'Hispanic or Latino (United States of America)': 'Hispanic',
                                    'Two or More Races (Not Hispanic or Latino) (United States of America)': 'Two or more',
                                    'White - Other (United Kingdom)': 'White',
                                    'White - Other European (United Kingdom)': 'White',
                                    'Asian (Indian) (India)': 'Asian',
                                    'Black - African (United Kingdom)': 'Black',
                                    'American Indian or Alaska Native (Not Hispanic or Latino) (United States of America)': 'American Indian',
                                    'White - British (United Kingdom)': 'White',
                                    'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino) (United States of America)': 'Pacific Islander'})
dni_value = 'dni'
new_terms['Ethnicity'] = new_terms['Ethnicity'].fillna(value=dni_value)
# label everything as either 'Prepare' or 'New'
#   this is just either 'Prepare,' or 'New'
new_terms['Prepare/New A'] = new_terms['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
                                                       'NXT': 'Prepare', 'Licensure Group': 'Prepare',
                                                       'Med': 'Prepare', 'Finance & Accounting': 'Prepare',
                                                       'Admissions Faculty': 'Prepare', 'Nursing': 'Prepare',
                                                       'MPrep': 'Prepare', 'Marketing': 'Prepare', 'Bar': 'Prepare',
                                                       'HR / PR / Admin': 'Prepare', 'Publishing': 'Prepare',
                                                       'Data and Learning Science': 'Prepare', 'Metis': 'New',
                                                       'Digital Media': 'Prepare', 'iHuman': 'New',
                                                       'Advise': 'New', 'International': 'Prepare',
                                                       'Metis Faculty': 'New', 'Admissions Core': 'Prepare',
                                                       'Admissions New': 'Prepare', 'Allied Health': 'Prepare',
                                                       'Legal': 'Prepare', 'TTL Labs': 'New', 'Executive': 'Prepare',
                                                       'Licensure Programs': 'Prepare'})
# this is either 'Prepare,' or the specific new business group
new_terms['Prepare/New B'] = new_terms['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
                                                       'NXT': 'Prepare', 'Licensure Group': 'Prepare',
                                                       'Med': 'Prepare', 'Finance & Accounting': 'Prepare',
                                                       'Admissions Faculty': 'Prepare', 'Nursing': 'Prepare',
                                                       'MPrep': 'Prepare', 'Marketing': 'Prepare', 'Bar': 'Prepare',
                                                       'HR / PR / Admin': 'Prepare', 'Publishing': 'Prepare',
                                                       'Data and Learning Science': 'Prepare', 'Metis': 'Metis',
                                                       'Digital Media': 'Prepare', 'iHuman': 'New',
                                                       'Advise': 'Advise', 'International': 'Prepare',
                                                       'Metis Faculty': 'Metis', 'Admissions Core': 'Prepare',
                                                       'Admissions New': 'Prepare', 'Allied Health': 'Prepare',
                                                       'Legal': 'Prepare', 'TTL Labs': 'DBC/TTL', 'Executive': 'Prepare',
                                                       'Licensure Programs': 'Prepare'})
# label everything into current digital/technology/marketing roles
new_terms['Digital'] = new_terms['Team'].map({'Analytics and Digital Marketing': 'Marketing',
                                                'Email Marketing': 'Marketing', 'Growth': 'Marketing',
                                                'Market Research': 'Marketing', 'Marketing Leadership': 'Marketing',
                                                'Cloud Operations': 'Technology', 'Data Engineering': 'Technology',
                                                'Delivery Management': 'Technology', 'MPrep Technology': 'Technology',
                                                'Platform': 'Technology', 'UX': 'Technology', 'Website': 'Technology',
                                                'Data Science': 'Data Analytics', 'Learning Science': 'Analytics',
                                                'Psychometrics': 'Data Analytics'})


new_terms['Digital B'] = new_terms['Team'].map({'Analytics and Digital Marketing': 'Digital',
                                                'Email Marketing': 'Digital', 'Growth': 'Digital',
                                                'Market Research': 'Digital', 'Marketing Leadership': 'Digital',
                                                'Cloud Operations': 'Digital', 'Data Engineering': 'Digital',
                                                'Delivery Management': 'Digital', 'MPrep Technology': 'Digital',
                                                'Platform': 'Digital', 'UX': 'Digital', 'Website': 'Digital',
                                                'Data Science': 'Digital', 'Learning Science': 'Digital',
                                                'Psychometrics': 'Digital'})

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

# save the updated termination spreadsheet

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\')
all_terms.to_csv('historic_terms.csv', index=False)

# todo: run the termination prediction stuff


# todo: create a separate frame of the rolling 12 month headcount metrics

#roll_12_date = records_date - timedelta(days=365)

#df_roll_12_terms = all_terms.loc[all_terms['Termination Date'] > roll_12_date]
#all_terms['roll_12_date'] = roll_12_date
#pd.to_datetime(all_terms['roll_12_date'])
#df_roll_12_terms = all_terms.loc[all_terms['roll_12_date'] > roll_12_date]

# isolate and save full time terms to a spreadsheet

ft_flt = all_terms['Time Type'] == 'Full time'
ft_terms = all_terms[ft_flt]
ft_terms.to_csv('historic_terms_ft.csv', index=False)

# send full time terms to a good spreadsheet

from oauth2client.service_account import ServiceAccountCredentials

print('\nPinging our Google overlords...')

os.chdir('C:\\USers\\DuEvans\\Documents\\ktp_data')
# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# find each datasheet and clear the current values
sheet = client.open("KTP Turnover Dashboard v1.0").worksheet('data')
sheet.clear()

# write each of the sheets with new data
gspread_dataframe.set_with_dataframe(sheet, ft_terms)

# get today's date, again, but format it as mm/dd/yyyy, and include the time
dt = datetime.now()
dt_pretty = dt.strftime("%m/%d/%y %I:%M%p")

# format into a string
last_update = 'Data updated at: ' + dt_pretty + '.'
print('\n')
print(last_update)

print('\Process finished.')
