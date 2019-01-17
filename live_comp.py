import pandas as pd
import numpy as np
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
import shutil
import os
from datetime import datetime, timedelta, date
from sys import exit
from oauth2client.service_account import ServiceAccountCredentials
from data_tagging import *
from gspread_pandas import Spread, Client
from gcred import gcred
from sqlalchemy import create_engine
import time
import requests
import json
pd.options.mode.chained_assignment = None  # default='warn'



def update_comp_dash(data, target_date):
    """
    Gets KIP targets, combines with full population
    pushes information to a google spreadsheet.
    :param data:
    :return:
    """
    os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\')
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    def get_kip_data():
        """Pulls KIP targets and creates total compensation targets."""
        time.sleep(5)
        kips = client.open('KIP 2018 Forecast').worksheet('2018 participants for editing')
        kips = kips.get_all_records()
        kips = pd.DataFrame.from_records(kips)

        # get only needed data
        kips = kips[['EEID', 'current_kip_pct', 'current_salary']]
        # rename
        kips = kips.rename(columns={'EEID': 'ID'})
        # drop missing values on the ID column
        kips = kips.loc[~kips['ID'].isin([''])]

        # create total comp column
        kips['total_comp'] = (kips['current_salary'] * kips['current_kip_pct']) + kips['current_salary']

        kips['kip_target'] = (kips['current_salary'] * kips['current_kip_pct'])

        return kips


    def merge_kip_data(data, kips):
        """Merges KIP and non-KIP datasets, so that each individual
        has a total compensation target."""

        #  merge the kip list with the indicator field on
        data = pd.merge(data, kips, on='ID', how='left', indicator=True)

        # 'both' is someone on a kip
        # 'left_only' is someone not on a kip
        on_kips = data.loc[data['_merge'] == 'both']
        off_kips = data.loc[data['_merge'] == 'left_only']

        # create total comp column for non-kips
        off_kips['total_comp'] = off_kips['Total Base Pay Annualized - Amount']
        off_kips['current_salary'] = off_kips['Total Base Pay Annualized - Amount']

        # combine dataframes
        new_data = pd.concat([on_kips, off_kips])

        # drop the merge column
        new_data = new_data.drop(columns='_merge')

        return new_data

    # todo: convert european currencies to USD
    #def convert_usd(new_data):
     #   url = 'https://api.exchangeratesapi.io/latest?symbols=USD,GBP'
      #  url2 = 'https://api.exchangeratesapi.io/latest?symbols=USD,EUR'

        # list of employees paid in euros:
    df_eur = ['P000186065']
    # list of employees paid in GBP:
    df_gdp = ['P000253854', 'P000186057', 'P000208553', 'P000291792',
              'P000297461', 'P000299209', 'P000263059', 'P000266483',
              'P000303662', 'P000299208', 'P000270907', 'P000304701',
              'P000310964', 'P000297620', 'P000296053']

    # response = requests.get(url)
       # rates = response.text


    def update_comp_gspread(data):
        # push information to google spreadsheet
        # remove advise
        data = data.loc[~data['Structure B'].isin(['Advise'])]
        data = data.loc[~data['Structure'].isin(['KIE', 'KU'])]
        # temporary test - data studio can't find the ID field
        data['EID'] = data['ID']
        # send to sheet
        gs1 = client.open('calc_comp_all_ktp').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, data)

    # todo: create on-going payroll summary statistics
    #  per-capita comp, total base comp, total target comp,
    #   by major operating units,


    def analyze_wage_gaps_management(data, target_date):
        """calculate the wage gaps, by gender, by management level
        index by each date."""

        # save with a date time index
        date_recorded = datetime.strptime(target_date, '%m_%d_%Y')

        # filter to male/female
        df = data.loc[data['Gender'].isin(['Male', 'Female'])]

        # pivot by gender - index = management levels
        df['current_salary'] = pd.to_numeric(df['current_salary'])
        df['total_comp'] = pd.to_numeric(df['total_comp'])
        dfp = pd.pivot_table(df, values=['current_salary', 'total_comp'],
                             index='Management Level A', columns=['Gender'],
                             aggfunc=(np.average, np.median))

        # create DataFrame
        dfp = pd.DataFrame(dfp.to_records())


        # calculate gender gaps
        # todo: repeat this by OU (if requested)
        # average gender gap in base compensation
        dfp['avg_base_gap'] = 1 + ((dfp["('current_salary', 'average', 'Female')"] -
                         dfp["('current_salary', 'average', 'Male')"]) /
                         dfp["('current_salary', 'average', 'Male')"])

        # median gender gap in base compensation
        dfp['med_base_gap'] = 1 + ((dfp["('current_salary', 'median', 'Female')"] -
                                    dfp["('current_salary', 'median', 'Male')"]) /
                                   dfp["('current_salary', 'median', 'Male')"])

        # average gender gap in total target compensation
        dfp['avg_tcomp_gap'] = 1 + ((dfp["('total_comp', 'average', 'Female')"] -
                                    dfp["('total_comp', 'average', 'Male')"]) /
                                   dfp["('total_comp', 'average', 'Male')"])

        # median gender gap in total target compensation
        dfp['med_tcomp_gap'] = 1 + ((dfp["('total_comp', 'median', 'Female')"] -
                                    dfp["('total_comp', 'median', 'Male')"]) /
                                   dfp["('total_comp', 'median', 'Male')"])

        # set index on the recorded date
        dfp['date_recorded'] = date_recorded
        dfp = dfp.set_index(['date_recorded'])

        wage_gaps_management = dfp

        return wage_gaps_management

    def analyze_wage_gaps_all(data, target_date):
        # repeat above with all full time KTP
        #  this is a separate dataframe - still indexed by date

        # save with a date time index
        date_recorded = datetime.strptime(target_date, '%m_%d_%Y')

        # filter to male/female
        df = data.loc[data['Gender'].isin(['Male', 'Female'])]

        # pivot by gender - index = management levels
        df['current_salary'] = pd.to_numeric(df['current_salary'])
        df['total_comp'] = pd.to_numeric(df['total_comp'])

        # df is the starting dataframe
        # all-in wage gaps
        dfa = pd.pivot_table(df, values=['current_salary', 'total_comp'],
                             columns=['Gender'], aggfunc=(np.average, np.median))

        dfa['all_gap'] = 1 + ((dfa['Female'] - dfa['Male']) / dfa['Male'])
        dfa = pd.DataFrame(dfa.to_records())

        # set index on the recorded date
        dfa['date_recorded'] = date_recorded
        dfa = dfa.set_index(['date_recorded'])

        wage_gaps_all = dfa

        return wage_gaps_all


    def update_wage_gap_management_data(data):
        """updates the data base holding historic wage gap information
        across management levels"""
        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///management_wage_gap_records.db', echo=False)

        # append new data to the database
        data.to_sql('management_wage_gap_records', con=engine, if_exists='append')


    def update_wage_gap_management_gspread():
        """updates the management level gspread
        that feeds into the compensation dashboard"""

        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///management_wage_gap_records.db', echo=False)

        # retrieve the dataframe
        #  can't use index column - won't write to google sheets
        df = pd.read_sql_query("SELECT * FROM management_wage_gap_records", con=engine)
        df = df.drop_duplicates()


        # write to the google spreadsheet
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\')
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
        client = gspread.authorize(creds)
        time.sleep(5)
        gs1 = client.open('management_gender_wage_gap').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, df)

    def update_wage_gap_all_data(data):
        """updates the data base holding historic wage gap information
            across all ktp"""
        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///all_wage_gap_records.db', echo=False)

        # append new data to the database
        data.to_sql('all_wage_gap_records', con=engine, if_exists='append')


    def update_wage_gap_all_gspread():
        """updates the all-in gspread
            that feeds into the compensation dashboard"""

        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///all_wage_gap_records.db', echo=False)

        # retrieve the dataframe
        #  can't use index column - won't write to google sheets
        df = pd.read_sql_query("SELECT * FROM all_wage_gap_records", con=engine)
        df = df.drop_duplicates()

        # write to the google spreadsheet
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\')
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
        client = gspread.authorize(creds)
        time.sleep(5)
        gs1 = client.open('all_gender_wage_gap').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, df)

    # todo: create compensation metrics
    #  total base salary, total kip targets, total target compensation,
    #  per capita base, total comp
    #  try by operating unit
    def analyze_comp_metrics(data):
        """creates compensation metrics for each day."""
        # save with a date time index
        date_recorded = datetime.strptime(target_date, '%m_%d_%Y')

        df = data

        # pivot by gender - index = management levels
        df['current_salary'] = pd.to_numeric(df['current_salary'])
        df['total_comp'] = pd.to_numeric(df['total_comp'])
        df['kip_target'] = pd.to_numeric(df['kip_target'])

        # df is the starting dataframe
        # all-in wage gaps
        dfcm = pd.pivot_table(df, values=['current_salary', 'total_comp', 'kip_target'],
                             columns=['Management Level A'],
                             aggfunc=(np.average, np.median, np.sum))
        dfcm['total'] = (dfcm['Above VP'] + dfcm['Director']
                         + dfcm['Executive Director'] + dfcm['Individual Contributor']
                         + dfcm['Manager'] + dfcm['VP'])


        dfcm = pd.DataFrame(dfcm.to_records())

        # set index on the recorded date
        dfcm['date_recorded'] = date_recorded
        dfcm = dfcm.set_index(['date_recorded'])

        comp_metrics = dfcm

        return comp_metrics

    # save comp metrics to database
    def update_management_comp_metrics_data(data):
        """updates the data base holding historic wage gap information
            across all ktp"""
        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///management_comp_metrics.db', echo=False)

        # append new data to the database
        data.to_sql('management_comp_metrics', con=engine, if_exists='append')

    # send comp metrics to gspread
    def update_management_comp_metrics_gspread():
        """updates the all-in gspread
            that feeds into the compensation dashboard"""

        # orient to the directory
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\compensation'
        os.chdir(file_path)

        # connect to the database
        engine = create_engine('sqlite:///management_comp_metrics.db', echo=False)

        # retrieve the dataframe
        #  can't use index column - won't write to google sheets
        df = pd.read_sql_query("SELECT * FROM management_comp_metrics", con=engine)
        df = df.drop_duplicates()

        # write to the google spreadsheet
        os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\')
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
        client = gspread.authorize(creds)
        time.sleep(5)
        gs1 = client.open('management_comp_metrics').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, df)

    # todo: calculate per-capita compensation
    def calc_per_capita_comp(data):
        """"""
        df = data

        # pivot by gender - index = management levels
        df['current_salary'] = pd.to_numeric(df['current_salary'])
        df['total_comp'] = pd.to_numeric(df['total_comp'])
        df['kip_target'] = pd.to_numeric(df['kip_target'])

        # pivot to create per capita compensation
        dfpc = pd.pivot_table()


    # create and import dataframe with 2016, 2017, 2018, and today's data
    def create_ft_historic_records():
        """
        Creates dataframes via 2016 - today's data.
        Imports KIP target and actual data
        Creates a single dataframe with all years labeled
        Pushes the data to a gspread
        :return:
        """

        # grab dataframes for 2016, 2017, 2018, and today
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\'

        f2016 = 'ktp_pop_01_01_2016.csv'
        f2017 = 'ktp_pop_01_01_2017.csv'
        f2018 = 'ktp_pop_01_01_2018.csv'
        ftoday = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + target_date + '.csv'

        df2016 = pd.read_csv(file_path + f2016)
        # add management level formatting to 2016 file
        df2016 = management_levels(df2016)
        df2017 = pd.read_csv(file_path + f2017)
        df2018 = pd.read_csv(file_path + f2018)
        dftoday = pd.read_csv(ftoday)

        # find kip target data
        client = gcred()

        br = client.open('bonus_records').worksheet('all_bonus')
        br = br.get_all_records()
        br = pd.DataFrame.from_records(br)

        # merge each of the dataframes with bonus data
        # create bonus records for each year
        br16 = br[['ID', 'total_bonus_16', 'paid_pct_16', 'tgt_16']]
        br17 = br[['ID', 'total_bonus_17', 'paid_pct_17', 'tgt_17']]
        br18 = br[['ID', 'total_bonus_18', 'paid_pct_18', 'tgt_18']]

        # rename to standardize for all years
        br16 = br16.rename(columns={'total_bonus_16': 'total_bonus',
                                    'paid_pct_16': 'bonus_paid_pct',
                                    'tgt_16': 'bonus_target'})
        br17 = br17.rename(columns={'total_bonus_17': 'total_bonus',
                                    'paid_pct_17': 'bonus_paid_pct',
                                    'tgt_17': 'bonus_target'})
        br18 = br18.rename(columns={'total_bonus_18': 'total_bonus',
                                    'paid_pct_18': 'bonus_paid_pct',
                                    'tgt_18': 'bonus_target'})

        df_2016 = pd.merge(df2016, br16, on='ID', how='left')
        df_2017 = pd.merge(df2017, br17, on='ID', how='left')
        df_2018 = pd.merge(df2018, br18, on='ID', how='left')
        df_today = pd.merge(dftoday, br18, on='ID', how='left')

        # create total paid compensation field for each year
        frames = [df_2016, df_2017, df_2018, df_today]
        cols = ['Total Base Pay Annualized - Amount', 'total_bonus',
                'bonus_paid_pct', 'bonus_target']
        for frame in frames:
            for col in cols:
                frame[col] = pd.to_numeric(frame[col])
                frame[col] = frame[col].fillna(0)
            frame['total_comp'] = frame['total_bonus'] + frame['Total Base Pay Annualized - Amount']
            frame['total_comp_tgt'] = frame['bonus_target'] + frame['Total Base Pay Annualized - Amount']
            frame['pct_tcomp_paid'] = frame['total_comp'] / frame['total_comp_tgt']

        # label each of the dataframes by the year
        # change these to datetimes
        df_2016['yr_index'] = pd.to_datetime("January 1st 2016").strftime('%d/%m/%Y')
        df_2016['yr_index_b'] = pd.to_datetime("January 1st 2016")
        df_2017['yr_index'] = pd.to_datetime("January 1st 2017").strftime('%d/%m/%Y')
        df_2017['yr_index_b'] = pd.to_datetime("January 1st 2017")
        df_2018['yr_index'] = pd.to_datetime("January 1st 2018").strftime('%d/%m/%Y')
        df_2018['yr_index_b'] = pd.to_datetime("January 1st 2018")
        date_recorded = datetime.strptime(target_date, '%m_%d_%Y').strftime('%m/%d/%Y')
        date_recorded_b = datetime.strptime(target_date, '%m_%d_%Y')

        df_today['yr_index'] = date_recorded
        df_today['yr_index_b'] = date_recorded_b


        # concat the dataframes into one
        hdf = pd.concat([df_2016, df_2017], sort=False)
        hdf = pd.concat([hdf, df_2018], sort=False)
        hdf = pd.concat([hdf, df_today], sort=False)


        # push the dataframe to a google spreadsheet
        gs1 = client.open('year_start_comp_data').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, hdf)


    # run function
    kips = get_kip_data()

    new_data = merge_kip_data(data, kips)

    update_comp_gspread(new_data)
    print('\nCompensation data updated.')

    # management level wage gaps
    wage_gaps_management = analyze_wage_gaps_management(new_data, target_date)

    update_wage_gap_management_data(wage_gaps_management)

    update_wage_gap_management_gspread()

    # all levels wage gaps
    wage_gaps_all = analyze_wage_gaps_all(new_data, target_date)

    update_wage_gap_all_data(wage_gaps_all)

    update_wage_gap_all_gspread()

    # calculate compensation metrics
    comp_metrics = analyze_comp_metrics(new_data)

    update_management_comp_metrics_data(comp_metrics)

    update_management_comp_metrics_gspread()

    print('\nWage gap data analyzed and updated.')

    # run year start compensation metrics calculation
    create_ft_historic_records()
    print('\nYear start compensation metrics updated.')

    print('\nComp dashboard updates complete.')

# temporary: create backlog of a few dates for the data to flow through

#os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\test_gaps')
#files = os.listdir()

#for file in files:
#    d = file[8:]
#    rdate = d[:10]
#    print(rdate)
#    time.sleep(5)
#    df0 = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\' + file)
#    df0 = df0.loc[~df0['Structure'].isin(['KU', 'KIE', 'Advise'])]
#    update_comp_dash(df0, rdate)

# todo: remove all the sleep calls
# todo: remove the data at the bottom
# todo: unsilence the key code pieces

def create_comp_changes_data(target_date):
    """reads the compensation changes data from workday."""
    filename = ('C:\\Users\\DuEvans\\Downloads\\comp_changes_' + target_date + '.xlsx')
    df = pd.read_excel(filename, skiprows=1)

    # temporary!
    #  pull in EID and MID fields into the report from population records
    def query_all_pop_records():
        file_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\'
        os.chdir(file_path)

        # connect to the database
        all_engine = create_engine('sqlite:///all_ktp_db', echo=False)

        df_all = pd.read_sql_query("SELECT * FROM all_ktp", con=all_engine)
        return df_all

    pop0 = query_all_pop_records()
    df_all = pop0.drop_duplicates(subset='ID')

    p1 = df_all[['ID', 'Manager ID', 'Preferred Name in General Display Format']]
    p1 = p1.rename(columns={'Preferred Name in General Display Format': 'Employee'})

    # merge
    df = pd.merge(df, p1, on='Employee', how='left', indicator=True)

    def format_comp_change_data(df):
        """"""
        # clean that data up!
        # drop un-needed columns

        # remove non-completed fields on status column
        df = df.loc[df['Status'] == 'Successfully Completed']

        # filter out hires (just internal moves)
        df = df.loc[~df['Business Process Name'].isin(['Propose Compensation Hire'])]

        # create a year field
        df['Effective Date'] = pd.to_datetime(df['Effective Date'])
        df['year'] = df['Effective Date'].dt.year

        # annualize all compensation fields
        df['annual_prior'] = np.where(df['Frequency'] == 'Hourly', (
            df['Current Base Pay'] * 2080), df['Current Base Pay'])

        df['annual_post'] = np.where(df['Frequency.1'] == 'Hourly', (
            df['Proposed Base Pay'] * 2080), df['Proposed Base Pay'])

        # create compensation delta field
        df['delta'] = df['annual_post'] - df['annual_prior']
        df['delta'] = df['delta'].round(decimals=0)
        # trim the dataframe so that outliers on comp delta are moved to 0
        df.loc[df.delta > 50000, 'delta'] = 0
        df.loc[df.delta < -10000, 'delta'] = 0

        df = df.loc[df['delta'] >= 50 ]

        # create compensation percent delta field
        df['pct_delta'] = df['delta'] / df['annual_prior']

        # identify promotions
        df['promotion'] = np.where(df['Reason'].str.contains('Promotion'), 'Promotion', 'Non-Promotion')

        # remove promotions where the comp delta is 0
        df.loc[df.delta == 0, 'promotion'] = 'Non-Promotion'

        # create separate dataframe that removes 0, inf, and nan comp deltas
        df1 = df.loc[df['delta'] != 0]


        df1['check_rates'] = np.where(df1['Frequency'] == df1['Frequency.1'],
                                      'match', 'diff')

        return df1

    # format data
    df1 = format_comp_change_data(df)

    def create_population(pop0):
        """creates dataframe of full time population"""
        p = pop0.loc[pop0['record_date'] == '2018-10-31 00:00:00.000000']
        p = p.loc[p['FT / PT'] == 'Full time']
        p0 = p[['ID', 'record_date']]
        return p, p0

    p, p0 = create_population(pop0)


    def create_change_frames(df1, p, p0):
        """
        returns three dataframes:
        last increase, last promotion, last merit
        :param df1:
        :return:
        """


        # dataframe of promotions
        df_promo = df1.loc[df1['promotion'] == 'Promotion']
        df_promo = df_promo.sort_values(by='delta')

        # create dataframe of employee, date of last promotion,
        #  percent increase of last promo, total dollars of last promo
        last_promo = df_promo[['ID', 'Effective Date', 'delta', 'pct_delta', 'year']]
        last_promo = last_promo.rename(columns={'Effective Date': 'date_promotion',
                                                'delta': 'promotion_dollar_increase',
                                                'pct_delta': 'promotion_percent_increase',
                                                'year': 'last_promotion_year'})

        #  sort values by most recent date
        #   drop duplicates on the employee ID, keeping first entry
        #   this creates most recent change
        lpromo = last_promo.sort_values(by='date_promotion', ascending=False)
        lpromo = lpromo.drop_duplicates(subset='ID', keep='first')

        # matching lpromo to population
        lp = pd.merge(p0, lpromo, on='ID', how='left', indicator=True)
        lp = lp.sort_values(by='date_promotion', ascending=False)

        # calculate days since last promotion
        lp['record_date'] = pd.to_datetime(lp['record_date'])
        lp['days_last_promo'] = ((lp['record_date']) - (lp['date_promotion'])).dt.days

        # drop unneeded columns
        lp = lp.drop(columns=['record_date', '_merge'])


        # create dataframe of employee, date of last increase,
        #  percent increase, and total dollar increase
        df_merit = df1.loc[df1['Reason'] == 'Request Compensation Change > Base Salary Change > Merit']
        last_merit = df_merit[['ID', 'Effective Date', 'delta', 'pct_delta', 'year']]
        last_merit = last_merit.rename(columns={'Effective Date': 'date_merit',
                                                'delta': 'merit_dollar_increase',
                                                'pct_delta': 'merit_percent_increase',
                                                'year': 'last_merit_year'})

        # calculate last merit increase
        lmerit = last_merit.sort_values(by='date_merit', ascending=False)
        lm = pd.merge(p0, lmerit, on='ID', how='left', indicator=True)
        lm = lm.sort_values(by='date_merit', ascending=False)
        lm = lm.drop_duplicates(subset='ID', keep='first')

        lm['record_date'] = pd.to_datetime(lm['record_date'])

        # date of last merit increase
        lm['days_since_last_merit'] = ((lm['record_date']) - (lm['date_merit'])).dt.days

        # drop unneeded columns
        lm = lm.drop(columns=['record_date', '_merge'])

        # create just increase dataframe
        last_increase = df1[['ID', 'Effective Date', 'delta', 'pct_delta', 'year',
                             'Reason', 'promotion']]
        last_increase = last_increase.rename(columns={'Effective Date': 'date_increase',
                                                      'delta': 'total_dollar_increase',
                                                      'pct_delta': 'total_percent_increase',
                                                      'Reason': 'last_increase_reason',
                                                      'year': 'last_increase_year'})
        last_increase.loc[last_increase.total_percent_increase == np.inf, 'total_percent_increase'] = 0



        # todo: find last compensation change (any reason)
        li = pd.merge(p0, last_increase, on='ID', how='left', indicator=True)
        li = li.sort_values(by='date_increase', ascending=False)
        li = li.drop_duplicates(subset='ID', keep='first')
        li['record_date'] = pd.to_datetime(li['record_date'])

        # calculate time since last increase
        li['days_last_increase'] = ((li['record_date']) - (li['date_increase'])).dt.days

        # drop unneeded columns
        li = li.drop(columns=['record_date', '_merge'])


        return lp, lm, li

    lp, lm, li = create_change_frames(df1, p, p0)

    # todo: find all comp changes in one dataframe
    # merge each data frame onto the master list



    # todo: match back all population data
    def merge_to_pop(p, promo, merit, increase):
        """"""
        # create simple population data frame
        pop1 = p[['ID', 'record_date']]

        # merge last promotion
        pop2 = pd.merge(pop1, promo, on='ID', how='left')
        # merge last merit
        pop3 = pd.merge(pop2, merit, on='ID', how='left')
        # merge last increase
        pop4 = pd.merge(pop3, increase, on='ID', how='left')

        # merge back on population data
        pop5 = pd.merge(p, pop4, on='ID', how='left')

        return pop5

    changes = merge_to_pop(p, lp, lm, li)

    # push the changes data to a dataframe
    client = gcred()
    gs1 = client.open('last_increase_ktp_today').sheet1
    gs1.clear()
    gspread_dataframe.set_with_dataframe(gs1, changes)


    # map all compensation change data to population records
    def map_comp_change_data(comp, pop):
        """"""

        pop = pop[['ID', 'FT / PT', 'Job Profile (Primary)', 'Business Title (Primary)',
                   'Management Level A', 'Manager ID', 'Worker\'s Manager(s)',
                   'Ethnicity', 'Gender', 'Age', 'Age Bracket', 'days_tenure',
                   'yrs_tenure', 'Structure', 'Structure B', 'yrs_tenure_group',
                   'di_leader', 'di_poc', 'Prepare/New A', 'Prepare/New B',
                   'Digital A', 'Digital B', 'Activity', 'Process', 'Category']]

        mapped = pd.merge(comp, pop, on='ID', how='left')
        return mapped

    mapped = map_comp_change_data(df1, p)


    # todo: pull historic population data from the compensation dashboard
    # todo: combine all years population data into single frame
    def update_comp_dash_population():
        # read each population sheet
        client = gcred()
        # 2015 data
        gs1 = client.open('comp_dashboard_backend').worksheet('pop_2015')
        p2015 = gs1.get_all_records()
        p2015 = pd.DataFrame.from_records(p2015)
        p2015['data_year'] = '2015'
        # 2016 data
        gs2 = client.open('comp_dashboard_backend').worksheet('pop_2016')
        p2016 = gs2.get_all_records()
        p2016 = pd.DataFrame.from_records(p2016)
        p2016['data_year'] = '2016'
        # 2017 data
        gs3 = client.open('comp_dashboard_backend').worksheet('pop_2017')
        p2017 = gs3.get_all_records()
        p2017 = pd.DataFrame.from_records(p2017)
        p2017['data_year'] = '2017'
        # 2018 data
        gs4 = client.open('comp_dashboard_backend').worksheet('pop_2018')
        p2018 = gs4.get_all_records()
        p2018 = pd.DataFrame.from_records(p2018)
        p2018['data_year'] = '2018'
        # 2019 data
        gs5 = client.open('comp_dashboard_backend').worksheet('pop_2019')
        p2019 = gs5.get_all_records()
        p2019 = pd.DataFrame.from_records(p2019)
        p2019['data_year'] = '2019'
        # most recent data
        gs6 = client.open('comp_dashboard_backend').worksheet('population_data')
        p_today = gs6.get_all_records()
        p_today = pd.DataFrame.from_records(p_today)
        p_today['data_year'] = 'today'

        # combine all population sheets
        dfh = p2015.append(p2016)
        dfh = dfh.append(p2017)
        dfh = dfh.append(p2018)
        dfh = dfh.append(p2019)
        dfh = dfh.append(p_today)

        # todo: trim to prepare
        prepare_h = dfh.loc[dfh['Prepare/New A'] == 'Prepare']
        prepare_h = prepare_h.loc[prepare_h['Structure B'].isin(['Admissions', 'Common',
                                                                 'NXT', 'Licensure'])]

        # push all population sheets to single historic list
        all_pop_sheet = client.open('comp_dashboard_backend').worksheet('pop_history')
        all_pop_sheet.clear()
        gspread_dataframe.set_with_dataframe(all_pop_sheet, prepare_h)

# todo: update bonus targets in the technology compensation spreadsheet
def update_tech_bonus_targets(tech_pop):
    # todo: pull list of bonus_2018 from bonus_records spreadsheet
    client = gcred()
    gs1 = client.open('bonus_records').worksheet('bonus_2018')
    tech_bonus = gs1.get_all_records()
    tech_bonus = pd.DataFrame.from_records(tech_bonus)
    tech_bonus = tech_bonus[['EEID', ' (Pro-Rated) KIP Target ', 'Current KIP %']]
    tech_bonus = tech_bonus.rename(columns={' (Pro-Rated) KIP Target ': 'pro_target', 'EEID': 'ID'})
    # silencing for now as this is all unused
    # todo: condense tech population to just be ID
    #tech_pop = tech_pop[['ID', 'record_date']]
    # todo: merge on to list of technology population
    #tech_bonus_pop = pd.merge(tech_pop, tech_bonus, on='ID', how='left')
    #tech_bonus_pop = tech_bonus_pop[['ID', 'pro_target', 'Current KIP %']]

    # todo: push info to backend tech comp spreadsheet
    #gs2 = client.open('Technology Group Compensation').worksheet('bonus_targets')
    #gs2.clear()
    #gspread_dataframe.set_with_dataframe(gs2, tech_bonus_pop)
