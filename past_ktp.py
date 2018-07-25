import pandas as pd
import numpy as np
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
import shutil
import os
from datetime import datetime
from datetime import date
from dateutil.parser import parse
from sys import exit


os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

target_date = input('\nWhat date are we looking for here? (mm_dd_yyyy) ')

file = ('C:\\Users\\DuEvans\\Downloads\\ktp_pop_' + target_date + '.xlsx')



# read the current population dataset
df0 = pd.read_excel(file, skiprows=7)

# save the raw record
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records\\')
file_name = 'ktp_raw_pop_' + target_date + '.csv'
df0.to_csv(file_name, index=False)

records_date = datetime.strptime(target_date, '%m_%d_%Y')

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

# filter to just full time
full_time = df0['FT / PT'] == 'Full time'
df = df0[full_time]

# read the manager key
mgr_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\meid_key.csv')
# read the eid key
eeid_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\eid_key.csv')
# format the eid key to match the manager map
eeid_key.columns = ['Name', 'ID', 'Structure', 'Group', 'Team']

df_2 = pd.merge(df, mgr_key, on='Manager ID', how='left')

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

# match service buckets
pop_mapped['days_tenure'] = (datetime.now() - (pop_mapped['(Most Recent) Hire Date'])).dt.days
pop_mapped['yrs_tenure'] = (pop_mapped['days_tenure']/365).round(0)
pop_mapped['months_tenure'] = (pop_mapped['yrs_tenure']/12).round(0)


# manually change a couple folks to be in the right labels:
pop_mapped.loc[pop_mapped['ID'] == 'P000238419', 'Management Level'] = '5 VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000018502', 'Management Level'] = '5 VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000055603', 'Management Level'] = '5 VP'
pop_mapped.loc[pop_mapped['ID'] == 'P000025952', 'Gender'] = 'Male'


# calculate age based on the date of birth field

hist_date = datetime.strptime(target_date, '%m_%d_%Y')

pop_mapped['days_old'] = (hist_date - (pop_mapped['Date of Birth (Locale Sensitive)'])).dt.days

pop_mapped['Age'] = (pop_mapped['days_old']/365).round(0)

# bin age into age ranges
age_bin_names = ['<25', '25 to 34', '35 to 44', '45 to 54', '55 to 64', '65+']
age_bins = [18, 24, 34, 44, 54, 64, 100]
pop_mapped['Age Bracket'] = pd.cut(pop_mapped['Age'], age_bins, labels=age_bin_names)

#today_date = pd.to_datetime(target_date, format='%d/%m/%Y')
#today_date = datetime.now().strftime("%m-%d_-%Y")

pop_mapped['DOB'] = pd.to_datetime(pop_mapped['Date of Birth (Locale Sensitive)'], format='%d/%m/%Y', errors='ignore')
pop_mapped['Age'] = hist_date - pop_mapped['DOB']

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


def find_unmatched():
    """
    Returns the individuals that need to be mapped against organizational structure.
    If none, saves the record.
    """

    def save_record():
        """creates a copy of the population headcount on the given date in subfolder"""
        new_filename = ('ktp_pop_' + target_date + '.csv')
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population')
        pop_mapped.to_csv(new_filename, index=False)
        print('\nRecord archived.')
    if count_na == 0:
        print('\nAll values matched, yay!')
        save_record()
    elif count_na != 0:
        print('\nMissing entries: ' + str(count_na) + ' employees; ' + str(mgr_missing) + ' managers.')
        for name in na_remaining['Worker\'s Manager(s)'].unique():
            print(name)
        for id in na_remaining['ID'].unique():
            print(id)
        print('\nExiting...')
        exit()

find_unmatched()


# organize the part time faculty
part_time = df0['FT / PT'] == 'Part time'
pt_data = df0[part_time]

# find the faculty based on job profile
faculty_data = pt_data.loc[pt_data['Job Profile (Primary)'].isin(['Instructor - Grad / COA PT', 'Instructor - PC PT', 'Instructor - NCLEX',
                                                                  'Instructor - Grad Canada PT', 'Instructor - Mprep', 'KTP UK Instructor'])]

# map ethnicity
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

print('\nProcess finished.')
