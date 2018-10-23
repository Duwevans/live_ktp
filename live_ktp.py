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
from oauth2client.service_account import ServiceAccountCredentials
from data_tagging import *
from update_historic_population_records import *
from di_indices import calc_diversity_indices
from comp_dashboard import update_comp_dash

pd.options.mode.chained_assignment = None  # default='warn'

total_time = time.time()

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

target_date = input('\nWhat is the target date? (mm_dd_yyyy) ')

file = ('C:\\Users\\DuEvans\\Downloads\\ktp_pop_' + target_date + '.xlsx')
records_date = datetime.strptime(target_date, '%m_%d_%Y')


# read the current population dataset
df0 = pd.read_excel(file, skiprows=7)

# save the total population, as is, no changes for now
os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records')
file_name = 'ktp_raw_pop_' + target_date + '.csv'
df0.pop('CC Hierarchy')
df0.to_csv(file_name, index=False)

# format df0 to be entire population (FT + PT) for more complete demographic records match
df1 = df0
df1 = manager_map(df1)
df1 = map_ethnicity(df1)
df1 = map_age_fields(df1, records_date)
df1 = map_tenure_fields(df1, records_date)
df1 = management_levels(df1)
df1 = prepare_or_new(df1)
df1 = digital_map(df1)
df1 = brm_map(df1)
df1 = hierarchy_id_match(df1)
df1['record_date'] = records_date
df1['record_date'] = pd.to_datetime(df1['record_date'])
df1 = df1.drop_duplicates(subset=['ID'], keep='last')

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp')
file_name = 'ktp_all_pop_' + target_date + '.csv'
df1.to_csv(file_name, index=False)

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

#df0 = pd.read_excel('C:\\Users\\DuEvans\\Downloads\\ktp_pop_07_16_2018.xlsx', skiprows=7)
# filter to just full time
full_time = df1['FT / PT'] == 'Full time'
df2 = df1[full_time]

df2 = df2.drop_duplicates(subset=['ID'], keep='first')

print('KTP mapped.')

# find any missing values
na_remaining = df2[df2['Structure'].isnull()]
mgr_missing = na_remaining['Manager ID'].nunique()
count_na = na_remaining['Manager ID'].count()


def find_unmatched(dataset):
    """Returns the number of missing values against the manager map, otherwise, saves the record."""

    def save_record(dataset):
        """creates a copy of the population headcount on the given date in subfolder"""
        new_filename = ('ktp_pop_' + target_date + '.csv')
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp')
        dataset.to_csv(new_filename, index=False)
        print('Record archived.')
    if count_na == 0:
        print('All values matched, yay!')
        save_record(dataset)

    elif count_na != 0:
        print('Missing entries: ' + str(count_na) + ' employees; ' + str(mgr_missing) + ' managers.')
        for name in na_remaining['Worker\'s Manager(s)'].unique():
            print(name)
        for id in na_remaining['ID'].unique():
            print(id)
        continue_input = input('\nContinue to save data without all matches present? (y/n) ')
        if continue_input == 'y':
            save_record(dataset)
        elif continue_input == 'n':
            print('\nExiting...')
            exit()

find_unmatched(df2)


# update historic records labeled by date
update_records = input('\nUpdate historic databases? (y/n) ')
if update_records == 'y':
    update_population_records(df2, df1)
elif update_records == 'n':
    pass

conf_population = df1

# send the data to a google spread sheet

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
ktp_data_non_conf = pop_non_conf.loc[pop_non_conf['Structure'].isin(['Admissions', 'Licensure', 'Common', 'New Ventures', 'Executive'])]
admissions_data_non_conf = pop_non_conf.loc[pop_non_conf['Structure'].isin(['Admissions'])]
admissions_nf_data_non_conf = admissions_data_non_conf.loc[~admissions_data_non_conf['CF Job Family Only Text (sans Job Group)'].isin(['Instructors'])]
ad_academics_nf_data_non_conf = admissions_nf_data_non_conf.loc[admissions_nf_data_non_conf['Team'].isin(['Admissions Academics'])]
licensure_data_non_conf = pop_non_conf.loc[pop_non_conf['Structure'].isin(['Licensure'])]
common_data_non_conf = pop_non_conf.loc[pop_non_conf['Structure'].isin(['Common'])]
common_no_nxt_non_conf = common_data_non_conf.loc[~common_data_non_conf['Team'].isin(['NXT Service', 'NXT Shared', 'Balance Resolution', 'SST'])]
new_ventures_data_non_conf = pop_non_conf.loc[pop_non_conf['Structure'].isin(['New Ventures'])]
prepare_data_non_conf = pop_non_conf.loc[pop_non_conf['Prepare/New A'] == 'Prepare']

ktp_data_conf = pop_conf.loc[pop_conf['Structure'].isin(['Admissions', 'Licensure', 'Common', 'New Ventures', 'Executive'])]

# find the faculty based on job profile
faculty_data = pop_non_conf.loc[pop_non_conf['Job Profile (Primary)'].isin(['Instructor - Grad / COA PT', 'Instructor - PC PT', 'Instructor - NCLEX',
                                                                  'Instructor - Grad Canada PT', 'Instructor - Mprep', 'KTP UK Instructor',
                                                                  'Instructor - Grad / COA FT', 'Instructor - Grad Canada FT',
                                                                  'Instructor - PC FT', 'Instructor - PC K12'])]



# create pivoted headcount on Structure B for turnover dashboard
turnover_hc = pd.pivot_table(df2, values=['ID'], index='Structure B', aggfunc=len)
turnover_hc = pd.DataFrame(turnover_hc.to_records())

digital_hc = pd.pivot_table(df2, values=['ID'], index='Digital A', aggfunc=len)
digital_hc = pd.DataFrame(digital_hc.to_records())

# make sure google sheet should be updated
# important to allow for 'n' input as I might be fixing past records
update_sheets = input('\nUpdate current population google spreadsheets? (y/n) ')
if update_sheets == 'y':
    google_time_1 = time.time()

    print('\nHear me, oh Great Google overseers...')

    # use creds to create a client to interact with the Google Drive API
    os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # find each datasheet, clear the current values, write new data

    ktp_dashboard = client.open('Prepare Demographic Dashboard v1.3').worksheet('KTP Data')
    ktp_dashboard.clear()
    gspread_dataframe.set_with_dataframe(ktp_dashboard, prepare_data_non_conf)


    all_ktp_dashboard = client.open('KTP All-In Demographic Dashboard v1.3').worksheet('All KTP Data')
    all_ktp_dashboard.clear()
    gspread_dataframe.set_with_dataframe(all_ktp_dashboard, ktp_data_non_conf)
    print('\nKTP-wide dashboard updated.')

    hr_team_dashboard = client.open('prepare_today').sheet1
    hr_team_dashboard.clear()
    gspread_dataframe.set_with_dataframe(hr_team_dashboard, prepare_data_non_conf)
    print('\nHR team dashboard updated.')

    di_progress = client.open('D&I Progress v1.0').worksheet('Prepare Today')
    di_progress.clear()
    gspread_dataframe.set_with_dataframe(di_progress, prepare_data_non_conf)

    di_progress_draft = client.open('D&I Progress v1.1').worksheet('Prepare Today')
    di_progress_draft.clear()
    gspread_dataframe.set_with_dataframe(di_progress_draft, prepare_data_non_conf)
    print('\nD&I Progress Dashboard updated.')

    # update terminations dashboard with pivoted 'Structure B' data
    turnover_sheet = client.open('Prepare Turnover Dashboard v1.0').worksheet('Today Headcount')
    turnover_sheet.clear()
    gspread_dataframe.set_with_dataframe(turnover_sheet, turnover_hc)

    digital_sheet = client.open('Prepare Turnover Dashboard v1.0').worksheet('Digital Headcount')
    digital_sheet.clear()
    gspread_dataframe.set_with_dataframe(digital_sheet, digital_hc)
    print('\nTurnover dashboard updated.')

    from updated_admissions_faculty import update_admissions_dashboard

    update_admissions_dashboard(target_date, records_date)

    # todo: update compensation dashboard
    update_comp_dash(df2)

    # send info to the bonus records sheet
    df2['Total Base Pay Annualized - Amount'] = pd.to_numeric(df2['Total Base Pay Annualized - Amount'])
    comp_data = df2[['ID', 'Total Base Pay Annualized - Amount']]

    bonus_records = client.open('bonus_records').worksheet('pop_today')
    bonus_records.clear()
    gspread_dataframe.set_with_dataframe(bonus_records, comp_data)

    print('\nCompensation dashboards updated.')

    # update the manager map google sheet with new information
    map_sheet = client.open('The Map v2').worksheet('population')
    map_sheet.clear()
    gspread_dataframe.set_with_dataframe(map_sheet, ktp_data_non_conf)

    map_draft = client.open('The Map v3').worksheet('population')
    map_draft.clear()
    gspread_dataframe.set_with_dataframe(map_draft, ktp_data_non_conf)
    print('\nManager map updated with current population.')

    # get today's date, again, but format it as mm/dd/yyyy, and include the time
    dt = datetime.now()
    dt_pretty = dt.strftime("%m/%d/%y %I:%M%p")

    # format into a string
    last_update = 'Data updated at: ' + dt_pretty + '.'
    print('\n')
    print(last_update)

    lap_time_1 = time.time() - google_time_1

    print('\nTime to update google spreadsheets: ' + time.strftime("%H:%M:%S", time.gmtime(lap_time_1)))

    # send the string to the google sheet
    last_updated_sheet = client.open("demographic visuals data").worksheet('last_updated')
    last_updated_sheet.update_cell(1, 1, last_update)

    print('\nDemographic data sets updated in google sheet.')

elif update_sheets == 'n':
    pass

# run changing ktp if you're up for it
update_changes_1 = input('\nRun comparison to previous date for changes? (y/n) ')
if update_changes_1 == 'y':
    import changing_ktp
elif update_changes_1 == 'n':
    pass

# prompt running D&I indices calculation
run_indices = input('Run D&I indices calculations? (y/n) ')
if run_indices == 'y':
    calc_diversity_indices(target_date, df2)
elif run_indices == 'n':
    pass

complete_time = time.time() - total_time

print('\nTotal process time: ' + time.strftime("%H:%M:%S", time.gmtime(complete_time)))

print('\nProcess finished.')
