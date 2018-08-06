import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import os

pd.options.mode.chained_assignment = None  # default='warn'

file_date_a = input('\nWhat is the most recent target date? (mm_dd_yyyy) ')
file_date_b = input('\nWhat is the previous target date? (mm_dd_yyyy) ')

file_a = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records\\ktp_raw_pop_' + file_date_a + '.csv'
file_b = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records\\ktp_raw_pop_' + file_date_b + '.csv'


pop_1 = pd.read_csv(file_a, encoding='latin1')
pop_0 = pd.read_csv(file_b, encoding='latin1')

datasets = [pop_1, pop_0]


# todo: find the current file
#date1 = file_a[-14:]
#date2 = date1[:-4]
date_a = datetime.strptime(file_date_a, '%m_%d_%Y')

# todo: find the previous file
#date1 = file_b[-14:]
#date2 = date1[:-4]
date_b = datetime.strptime(file_date_b, '%m_%d_%Y')


# import the manager key

mgr_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\meid_key.csv')

# read the eid key
eeid_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\eid_key.csv')

# format the eid key to match the manager map

eeid_key.columns = ['Name', 'ID', 'Structure', 'Group', 'Team']

# read the brm key
brm_map = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\brm_map.csv', encoding='latin1')
brm_map['brm_key'] = brm_map['LOCAL_ACTIVITY_ID']
brm_map['brm_key'] = brm_map['brm_key'].str.lower()
brm_map = brm_map[['brm_key', 'Activity', 'Process', 'Category']]

def format_dataframes(datasets):

    for df in datasets:

        # add in the manager key
        df = pd.merge(df, mgr_key, on='Manager ID', how='left', sort=False)
        # remove nan values from the matched dataset

        mgr_nan = df[df['Structure'].isnull()]

        # remove the nan values from the original dataset
        mgr_mapped = df[df['Structure'].notnull()]

        # drop the nan columns
        mgr_nan = mgr_nan.drop(['Primary Key','Structure', 'Group', 'Team'], axis=1)

        # match the values to the EEID map
        eeid_mapped = pd.merge(mgr_nan, eeid_key, on='ID', how='left', sort=False)

        # compile the new dataset
        df = mgr_mapped.append(eeid_mapped, sort=False)

        df = df.rename(columns={'Primary Key': 'Manager'})



        # todo: import the brm key
        # create the needed field to map to BRM

        df['_1'] = df['Cost Centers'].str[:6]
        df['_2'] = df['_1'].str[:2]
        df['_3'] = df['_1'].str[-4:]
        df['_4'] = (df['_2'] + "_" + df['_3'])
        df['brm_key'] = (df['Single Job Family'] + "_" + df['_4'])
        df['brm_key'] = df['brm_key'].str.lower()
        df = df.drop(columns=['_1', '_2', '_3', '_4'])

        # read the amend the BRM file



        # merge the two files w/ BRM categories

        df = pd.merge(df, brm_map, on='brm_key', how='left', sort=False)

        # reset the index on ID
        #df = df.set_index('ID')


# format management levels
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


# set date of birth column
pop_1['dob'] = pd.to_datetime(pop_1['Date of Birth (Locale Sensitive)'])

pop_0['dob'] = pd.to_datetime(pop_0['Date of Birth (Locale Sensitive)'])

# set most recent hire date column
pop_1['doh'] = pd.to_datetime(pop_1['(Most Recent) Hire Date'])

pop_0['doh'] = pd.to_datetime(pop_0['(Most Recent) Hire Date'])


# map ethnicities
dni_value = 'dni'

pop_1['Ethnicity'] = pop_1['Race/Ethnicity (Locale Sensitive)'].map(
            {'White (Not Hispanic or Latino) (United States of America)': 'White',
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
pop_1['Ethnicity'] = pop_1['Ethnicity'].fillna(value=dni_value)

pop_0['Ethnicity'] = pop_0['Race/Ethnicity (Locale Sensitive)'].map(
            {'White (Not Hispanic or Latino) (United States of America)': 'White',
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
pop_0['Ethnicity'] = pop_0['Ethnicity'].fillna(value=dni_value)

format_dataframes(datasets)

# label the column names as new or previous


# add columns from both data frames on EID as index

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

# calculate age
pop_all['days_old_a'] = (date_a - (pop_all['dob_a'])).dt.days

pop_all['age_a'] = (pop_all['days_old_a']/365).round(0)

pop_all['days_old_b'] = (date_b - (pop_all['dob_b'])).dt.days

pop_all['age_b'] = (pop_all['days_old_b']/365).round(0)

# calculate tenure
pop_all['days_tenure_a'] = (date_a - (pop_all['doh_a'])).dt.days
pop_all['yrs_tenure_a'] = (pop_all['days_tenure_a']/365).round(0)
pop_all['months_tenure_a'] = (pop_all['yrs_tenure_a']/12).round(0)

pop_all['days_tenure_b'] = (date_b - (pop_all['doh_b'])).dt.days
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

# find those that were termed
#hired = pop_all.loc[pop_all['Manager ID_b'].isnull()]

# find those that were hired
#termed = pop_all.loc[pop_all['Manager ID_a'].isnull()]

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

df_changes['date'] = date_a
df_changes = df_changes.set_index('date')


# get the previous file,
#  and save an archived version of the previous file
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\archive')
old_changes = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\ktp_population_changes.csv')
records_name = 'ktp_population_changes_' + file_date_b + '.csv'
old_changes.to_csv(records_name)
print('\nRecords archived.')

# append the existing change dataframe with new data
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\changes\\')
updated_df = old_changes.append(df_changes, sort=False)
updated_df.to_csv('ktp_population_changes.csv', index=False)

print('\nProcess finished.')
