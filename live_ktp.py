import pandas as pd
import numpy as np
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
import shutil
import os
from datetime import datetime
from datetime import date
from sys import exit

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

target_date = input('\nWhat is the target date? (mm_dd_yyyy) ')

file = ('C:\\Users\\DuEvans\\Downloads\\ktp_pop_' + target_date + '.xlsx')
records_date = datetime.strptime(target_date, '%m_%d_%Y')


# read the current population dataset
df0 = pd.read_excel(file, skiprows=7)

# save the total population, as is, no changes for now
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records')
file_name = 'ktp_raw_pop_' + target_date + '.csv'
df0.to_csv(file_name, index=False)

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

#df0 = pd.read_excel('C:\\Users\\DuEvans\\Downloads\\ktp_pop_07_16_2018.xlsx', skiprows=7)
# filter to just full time
full_time = df0['FT / PT'] == 'Full time'
df_ft = df0[full_time]

# read the manager key
mgr_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\meid_key.csv')
# read the eid key
eeid_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\eid_key.csv')
# format the eid key to match the manager map
eeid_key.columns = ['Name', 'ID', 'Structure', 'Group', 'Team']

df_2 = pd.merge(df_ft, mgr_key, on='Manager ID', how='left')


# map against BRM categories

# create the needed field to map to BRM

df_2['_1'] = df_2['Cost Centers'].str[:6]
df_2['_2'] = df_2['_1'].str[:2]
df_2['_3'] = df_2['_1'].str[-4:]
df_2['_4'] = (df_2['_2'] + "_" + df_2['_3'])
df_2['brm_key'] = (df_2['Single Job Family'] + "_" + df_2['_4'])
df_2['brm_key'] = df_2['brm_key'].str.lower()
df_2 = df_2.drop(columns=['_1', '_2', '_3', '_4'])

# read the amend the BRM file

brm_map = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\brm_map.csv', encoding='latin1')
brm_map['brm_key'] = brm_map['LOCAL_ACTIVITY_ID']
brm_map['brm_key'] = brm_map['brm_key'].str.lower()
brm_map = brm_map[['brm_key', 'Activity', 'Process', 'Category']]


# merge the two files w/ BRM categories

df_2 = pd.merge(df_2, brm_map, on='brm_key', how='left')


# remove nan values from the matched dataset

mgr_nan = df_2[df_2['Structure'].isnull()]

# remove the nan values from the original dataset
mgr_mapped = df_2[df_2['Structure'].notnull()]


# drop the nan columns
mgr_nan = mgr_nan.drop(['Primary Key','Structure', 'Group', 'Team'], axis=1)


# match the values to the EEID map
eeid_mapped = pd.merge(mgr_nan, eeid_key, on='ID', how='left')

# compile the new dataset
pop_mapped = mgr_mapped.append(eeid_mapped)

pop_mapped = pop_mapped.rename(columns={'Primary Key': 'Manager'})
#remove that one dumb field
pop_mapped.pop('CC Hierarchy')

# match service buckets
pop_mapped['days_tenure'] = (records_date - (pop_mapped['(Most Recent) Hire Date'])).dt.days
pop_mapped['yrs_tenure'] = (pop_mapped['days_tenure']/365).round(0)
pop_mapped['months_tenure'] = (pop_mapped['yrs_tenure']/12).round(0)
# create age column

# calculate age based on the date of birth field
pop_mapped['days_old'] = (records_date - (pop_mapped['Date of Birth (Locale Sensitive)'])).dt.days

pop_mapped['Age'] = (pop_mapped['days_old']/365).round(0)

# bin age into age ranges
age_bin_names = ['<25', '25 to 34', '35 to 44', '45 to 54', '55 to 64', '65+']
age_bins = [18, 24, 34, 44, 54, 64, 100]
pop_mapped['Age Bracket'] = pd.cut(pop_mapped['Age'], age_bins, labels=age_bin_names)


yrs_ten_bins = [-1, 2, 4, 6, 8, 10, 100]

yrs_bin_names = ['0 to 2', '2 to 4', '4 to 6', '6 to 8', '8 to 10', '10+']
pop_mapped['yrs_tenure_group'] = pd.cut(pop_mapped['yrs_tenure'], yrs_ten_bins, labels=yrs_bin_names)

pop_mapped = pop_mapped.drop_duplicates(subset=['ID'], keep='first')
print('KTP mapped.')

# find any missing values
na_remaining = pop_mapped[pop_mapped['Structure'].isnull()]
mgr_missing = na_remaining['Manager ID'].nunique()
count_na = na_remaining['Manager ID'].count()


# map ethnicity into usable labels
pop_mapped['Ethnicity'] = pop_mapped['Race/Ethnicity (Locale Sensitive)'].map({'White (Not Hispanic or Latino) (United States of America)': 'White',
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
pop_mapped['Ethnicity'] = pop_mapped['Ethnicity'].fillna(value=dni_value)
print('Ethnicity mapped and organized.')

# reset management labels


pop_mapped['Management Level1'] = pop_mapped['Management Level'].map({'11 Individual Contributor': 'Individual Contributor',
                                                        '9 Manager': 'Manager', '8 Senior Manager': 'Manager',
                                                        '10 Supervisor': 'Manager', '7 Director': 'Director',
                                                        '6 Exec & Sr. Director/Dean': 'Executive Director',
                                                        '5 VP': 'VP', '4 Senior VP': 'Above VP',
                                                        '2 Senior Officer': 'Above VP',
                                                        '3 Executive VP': 'Above VP'})

# manually change a couple folks to be in the right labels:
pop_mapped.loc[pop_mapped['ID'] == 'P000238419', 'Management Level1'] = 'VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000018502', 'Management Level1'] = 'VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000055603', 'Management Level1'] = 'VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000025952', 'Gender'] = 'Male'


# label everything as either 'Prepare' or 'New'

# this is just either 'Prepare,' or 'New'
pop_mapped['Prepare/New A'] = pop_mapped['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
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
                                                       'Legal': 'Prepare', 'DBC/TTL': 'New', 'Executive': 'Prepare',
                                                       'Licensure Programs': 'Prepare'})

# this is either 'Prepare,' or the specific new business group
pop_mapped['Prepare/New B'] = pop_mapped['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
                                                       'NXT': 'Prepare', 'Licensure Group': 'Prepare',
                                                       'Med': 'Prepare', 'Finance & Accounting': 'Prepare',
                                                       'Admissions Faculty': 'Prepare', 'Nursing': 'Prepare',
                                                       'MPrep': 'Prepare', 'Marketing': 'Prepare', 'Bar': 'Prepare',
                                                       'HR / PR / Admin': 'Prepare', 'Publishing': 'Prepare',
                                                       'Data and Learning Science': 'Prepare', 'Metis': 'Metis',
                                                       'Digital Media': 'Prepare', 'iHuman': 'iHuman',
                                                       'Advise': 'Advise', 'International': 'Prepare',
                                                       'Metis Faculty': 'Metis', 'Admissions Core': 'Prepare',
                                                       'Admissions New': 'Prepare', 'Allied Health': 'Prepare',
                                                       'Legal': 'Prepare', 'DBC/TTL': 'DBC/TTL', 'Executive': 'Prepare',
                                                       'Licensure Programs': 'Prepare'})

# label everything into current digital/technology/marketing roles

pop_mapped['Digital'] = pop_mapped['Team'].map({'Analytics and Digital Marketing': 'Marketing',
                                                'Email Marketing': 'Marketing', 'Growth': 'Marketing',
                                                'Market Research': 'Marketing', 'Marketing Leadership': 'Marketing',
                                                'Cloud Operations': 'Technology', 'Data Engineering': 'Technology',
                                                'Delivery Management': 'Technology', 'MPrep Technology': 'Technology',
                                                'Platform': 'Technology', 'UX': 'Technology', 'Website': 'Technology'})




def find_unmatched():
    """Returns the """

    def save_record():
        """creates a copy of the population headcount on the given date in subfolder"""
        new_filename = ('ktp_pop_' + target_date + '.csv')
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp')
        pop_mapped.to_csv(new_filename, index=False)
        print('Record archived.')
    if count_na == 0:
        print('All values matched, yay!')
        save_record()
    elif count_na != 0:
        print('Missing entries: ' + str(count_na) + ' employees; ' + str(mgr_missing) + ' managers.')
        for name in na_remaining['Worker\'s Manager(s)'].unique():
            print(name)
        for id in na_remaining['ID'].unique():
            print(id)
        os.remove(file)
        print('\nExiting...')
        exit()

find_unmatched()


# send the data to a google spread sheet

from oauth2client.service_account import ServiceAccountCredentials

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')
pop = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + target_date + '.csv')
# remove that one dumb field
#pop.pop('CC Hierarchy')

pop_conf = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + target_date + '.csv')
#pop_conf.pop('CC Hierachy')

pop_non_conf = pop
# remove compensation fields - removing confidential information
pop_non_conf.pop('Total Base Pay - Amount')
pop_non_conf.pop('Total Base Pay Annualized - Amount')


# create the structure-specific datasets
ktp_data_non_conf = pop_non_conf[pop_non_conf['Structure'].isin(['Admissions', 'Licensure', 'Common', 'New Ventures', 'Executive'])]
admissions_data_non_conf = pop_non_conf[pop_non_conf['Structure'].isin(['Admissions'])]
licensure_data_non_conf = pop_non_conf[pop_non_conf['Structure'].isin(['Licensure'])]
common_data_non_conf = pop_non_conf[pop_non_conf['Structure'].isin(['Common'])]
new_ventures_data_non_conf = pop_non_conf[pop_non_conf['Structure'].isin(['New Ventures'])]

ktp_data_conf = pop_conf[pop_conf['Structure'].isin(['Admissions', 'Licensure', 'Common', 'New Ventures', 'Executive'])]


print('\nKTP full time employees mapped.')

# organize the part time faculty
part_time = df0['FT / PT'] == 'Part time'
pt_data = df0[part_time]

# map ethnicity


# find the faculty based on job profile
faculty_data = pt_data.loc[pt_data['Job Profile (Primary)'].isin(['Instructor - Grad / COA PT', 'Instructor - PC PT', 'Instructor - NCLEX',
                                                                  'Instructor - Grad Canada PT', 'Instructor - Mprep', 'KTP UK Instructor'])]

# remove compensation information
faculty_data.pop('Total Base Pay - Amount')
faculty_data.pop('Total Base Pay Annualized - Amount')

faculty_data['Ethnicity'] = faculty_data['Race/Ethnicity (Locale Sensitive)'].map({'White (Not Hispanic or Latino) (United States of America)': 'White',
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

faculty_data['days_old'] = (records_date - (faculty_data['Date of Birth (Locale Sensitive)'])).dt.days

faculty_data['Age'] = (faculty_data['days_old']/365).round(0)

faculty_filename = ('ktp_faculty_' + target_date + '.csv')
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\faculty')
pop_mapped.to_csv(faculty_filename, index=False)
print('Faculty records archived.')


# remove that one dumb field that makes the sheet look weird
faculty_data.pop('CC Hierarchy')


# make sure google sheet should be updated
# important to allow for 'n' input as I might be fixed past records
input = input('\nUpdate google spreadsheets? (y/n) ')
if input == 'y':
    pass
elif input == 'n':
    print('\nProcess finished.')
    exit()

print('\nHear me, oh Great Google overseers...')

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# find each datasheet and clear the current values
ktp_sheet = client.open("demographic visuals data").worksheet('ktp')
ktp_sheet.clear()
admissions_sheet = client.open("demographic visuals data").worksheet('admissions')
admissions_sheet.clear()
licensure_sheet = client.open("demographic visuals data").worksheet('licensure')
admissions_sheet.clear()
common_sheet = client.open("demographic visuals data").worksheet('common')
common_sheet.clear()
new_ventures_sheet = client.open("demographic visuals data").worksheet('new ventures')
new_ventures_sheet.clear()
faculty_sheet = client.open("demographic visuals data").worksheet('faculty')
faculty_sheet.clear()

# write to the compensation dashboard
compensation_sheet = client.open("Compensation Dashboard v1.0").worksheet('people data')
compensation_sheet.clear()


# write each of the sheets with new data
gspread_dataframe.set_with_dataframe(ktp_sheet, ktp_data_non_conf)
gspread_dataframe.set_with_dataframe(admissions_sheet, admissions_data_non_conf)
gspread_dataframe.set_with_dataframe(licensure_sheet, licensure_data_non_conf)
gspread_dataframe.set_with_dataframe(common_sheet, common_data_non_conf)
gspread_dataframe.set_with_dataframe(new_ventures_sheet, new_ventures_data_non_conf)
gspread_dataframe.set_with_dataframe(faculty_sheet, faculty_data)
gspread_dataframe.set_with_dataframe(compensation_sheet, ktp_data_conf)

# todo: update the manager map google sheet with new information



# get today's date, again, but format it as mm/dd/yyyy, and include the time
dt = datetime.now()
dt_pretty = dt.strftime("%m/%d/%y %I:%M%p")

# format into a string
last_update = 'Data updated at: ' + dt_pretty + '.'
print('\n')
print(last_update)
# send the string to the google sheet
last_updated_sheet = client.open("demographic visuals data").worksheet('last_updated')
last_updated_sheet.update_cell(1,1,last_update)

print('\nDemographic data sets updated in google sheet.')


# todo: calculate the gender diversity index for each OU, Group, and Team
# pop is the ft and formatted data set

print('\nCalculating diversity and gender indices...')

str_gdr = pd.pivot_table(pop, values=['ID'], index=['Structure'], columns=['Gender'], aggfunc=len)
str_gdr_df = pd.DataFrame(str_gdr.to_records())
str_gdr_df = str_gdr_df.fillna(0)
str_gdr_df = str_gdr_df.rename(columns={"('ID', 'Female')": 'female', "('ID', 'Male')": 'male'})
str_gdr_df['gdr_total'] = str_gdr_df['male'] + str_gdr_df['female']

str_gdr_df['pct_female'] = str_gdr_df['female'] / str_gdr_df['gdr_total']

# todo: calculate the ethnicity diversity index for each OU, Group, and Team

str_eth = pd.pivot_table(pop, values=['ID'], index=['Structure'], columns=['Ethnicity'], aggfunc=len)
str_eth_df = pd.DataFrame(str_eth.to_records())
str_eth_df = str_eth_df.fillna(0)
str_eth_df = str_eth_df.rename(columns={"('ID', 'American Indian')": 'american_indian', "('ID', 'Asian')": 'asian',
                                        "('ID', 'Black')": 'black', "('ID', 'Hispanic')": 'hispanic',
                                        "('ID', 'Pacific Islander')": 'pacific_islander', "('ID', 'Two or more')": 'two_or_more',
                                        "('ID', 'White')": 'white'})
str_eth_df['eth_total'] = (str_eth_df['american_indian'] + str_eth_df['asian'] + str_eth_df['black'] + str_eth_df['hispanic']\
                      + str_eth_df['pacific_islander'] + str_eth_df['two_or_more'] + str_eth_df['white'])

str_eth_df['non_white'] = (str_eth_df['american_indian'] + str_eth_df['asian'] + str_eth_df['black']
                        + str_eth_df['hispanic'] + str_eth_df['pacific_islander'] + str_eth_df['two_or_more'])

str_eth_df['pct_non_white'] = str_eth_df['non_white'] / str_eth_df['eth_total']

str_df = pd.merge(str_eth_df, str_gdr_df, on=['Structure'], how='left')

str_df = str_df[['Structure', 'female', 'male', 'gdr_total', 'pct_female', 'american_indian', 'asian', 'black',
                 'hispanic', 'pacific_islander', 'two_or_more', 'white', 'eth_total', 'pct_non_white']]


# todo: write the D&I indices to a google sheet as backend

print('\nProcess finished.\n')
