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


def update_google_term_data(data, records_date):
    """
    Updates the google spreadsheets that hold termination data.
    :param data:
    :return:
    """
    terms0 = data
    terms0 = terms0.drop_duplicates(subset=['ID'])
    terms0['Termination Date'] = pd.to_datetime(terms0['Termination Date'])
    terms_prepare = terms0.loc[terms0['Prepare/New A'] == 'Prepare']

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

    all_terms = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\terminations\\historic_terms.csv',
                            encoding='latin1')
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

    # D&I (confidential) dashboard
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
    # gspread_dataframe.set_with_dataframe(sheet, ft_terms)

    # get today's date, again, but format it as mm/dd/yyyy, and include the time
    dt = datetime.now()
    dt_pretty = dt.strftime("%m/%d/%y %I:%M%p")

    # format into a string
    last_update = 'Data updated at: ' + dt_pretty + '.'
    print('\n')
    print(last_update)

    print('Termination google sheets updated.')
