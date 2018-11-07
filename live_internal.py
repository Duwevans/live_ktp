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


# read and write records of all internal employee movement
#  promotions, transfers, increases

def map_internal_movement(target_date):
    """"""

    # temporary!
    #  pull in population file (unpassed from other function)
    pop_filename = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_' + target_date + '.csv'
    pop = pd.read_csv(pop_filename)
    pop = pop.loc[pop['Structure B'].isin(['New Ventures', 'Admissions', 'Common', 'Licensure', 'NXT'])]

    # grab new data file
    def get_new_data(target_date):
        """"""
        file_name = 'C:\\Users\\DuEvans\\Downloads\\int_moves_' + target_date + '.xlsx'
        df0 = pd.read_excel(file_name, skiprows=5)

        return df0

    def get_temp_eids():
        """temporary - imports EID field to the merit data"""
        # read the ft-only database
        db_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\'
        os.chdir(db_path)
        engine = create_engine('sqlite:///ft_ktp_db', echo=False)
        eids = pd.read_sql_query("SELECT * FROM ft_ktp", con=engine)

        # isolate to just names and IDs
        eids = eids[['ID', 'Preferred Name in General Display Format']]
        # rename name column to match merit file
        eids = eids.rename(columns={'Preferred Name in General Display Format':
                                    'Employee'})
        # return EIDs
        return eids

    eids = get_temp_eids()

    df0 = get_new_data(target_date)

    # todo: read existing data from gspread
    # todo: find only new data
    #def find_only_new_data(data):
       #""""""


    def format_new_data(data):
        """"""

        # change the employee ID label
        data = data.rename(columns={'Employee ID':'ID'})
        # remove hires from the data
        data = data.loc[~data['Business Process Type'].isin(['Hire'])]

        # add a year label
        data['effective_year'] = data['Effective Date'].dt.year

        # remove duplicate promotion entries (outbound is duplicate entry)
        data = data.loc[~data['Business Process Type'].isin(['Promote Employee Outbound'])]

        # remove part time from the data (current and proposed)
        data['time_types'] = np.where((data['Time Type - Current'] == 'Part time')
                                      & (data['Time Type - Proposed'] == 'Part time'),
                                      'part_time_both', 'full_time_one')
        data_1 = data.loc[data['time_types'] == 'full_time_one']

        # todo: import population data

        # todo: calculate the annual_prior field (worker's total base pay)
        data_1['annual_comp_prior'] = np.where(data_1['Pay Rate Type - Current'] == 'Hourly',
                                               data_1['Worker\'s Total Base Pay'] * 2080,
                                               data_1['Worker\'s Total Base Pay'])

        # todo: calculate the annual_post field (base pay - proposed)
        data_1['annual_comp_post'] = np.where(data_1['Pay Rate Type - Proposed'] == 'Hourly',
                                               data_1['Worker\'s Total Base Pay - Proposed'] * 2080,
                                               data_1['Worker\'s Total Base Pay - Proposed'])
        data_x = data_1.loc[data_1['annual_comp_prior'] != data_1['annual_comp_post']]
        # todo: take only cases where 'business process reason category text' != 'change contractor details'


        # create annual hourly rates for proposed comp
        # find roles where pay rate has changed
        data_1['pay_rate_change'] = np.where(data_1['Pay Rate Type - Current']
                                           == data_1['Pay Rate Type - Proposed'],
                                           'no_change', 'change')
        # create annualized 'post change' comp field
        data_1['annual_post'] = np.where((data_1['Pay Rate Type - Proposed']
                                       == 'Hourly') & (data_1['Worker\'s Total Base Pay - Proposed'] <= 100)
                                         & (data_1['Business Process Reason Text'] != 'Full-Time to Part-Time')
                                         & (data_1['Business Process Reason Text'] != 'Part-Time to Full-Time'),
                                         data_1['Worker\'s Total Base Pay - Proposed'] * 2080,
                                         data_1['Worker\'s Total Base Pay - Proposed'])

        # create annualized 'prior change' comp field
        #  have to use the subtracted amount from total base pay - proposed
        delta_field = 'CF Worker Compensation History Event- Base Pay Amount Changed In Last Event'
        data_1['annual_delta'] = np.where((data_1['Pay Rate Type - Proposed']
                                          == 'Hourly') & (data_1[delta_field] <=100)
                                          & (data_1['Business Process Reason Text'] != 'Full-Time to Part-Time')
                                          & (data_1['Business Process Reason Text'] != 'Part-Time to Full-Time'),
                                          data_1[delta_field] * 2080,
                                          data_1[delta_field])


        # calculate delta and percent delta
        data_1['annual_prior'] = data_1['annual_post'] - data_1['annual_delta']
        data_1['pct_delta'] = np.where(data_1['annual_delta'] > 0,
                                          (data_1['annual_delta'] / data_1['annual_prior']),
                                          np.nan)

        # temporary!
        #  testing what the post == prior values are
        (data_1['annual_prior'] == data_1['annual_post']).value_counts()
        data_x = data_1.loc[data_1['annual_prior'] == data_1['annual_post']]


        # filter such that:
        #  annual total dollar change can't be zero
        #  prior annual amount can't be zero or less
        data_1 = data_1.loc[data_1['annual_prior'] > 0]
        data_1 = data_1.loc[data_1['pct_delta'] >= .01]


        # custom label 'Business Process Reason Text' field
        #  promotions, internal, ft <> pt
        data_1['move_type_a'] = data_1['Business Process Reason Text'].map({
                                        'Move to another manager': 'Internal',
                                        'Additional Job': 'Internal',
                                        'Change in Position Name/Title': 'Internal',
                                        'Promotion': 'Promotion',
                                        'Change Job Details': 'Internal',
                                        'Change Location': 'Internal',
                                        'Change Job Profile': 'Internal',
                                        'Full-Time to Part-Time': 'Move PT',
                                        'Part-Time to Full-Time': 'Move FT',
                                        'Move to another position on my team': 'Internal',
                                        'Move to another company/business unit': 'Internal',
                                        'Move to another cost center/department': 'Internal'})
        data_1['move_type_b'] = data_1['Business Process Reason Text'].map({
                                        'Move to another manager': 'Lateral',
                                        'Additional Job': 'Add Job',
                                        'Change in Position Name/Title': 'Increase',
                                        'Promotion': 'Promotion',
                                        'Change Job Details': 'Increase',
                                        'Change Location': 'Increase',
                                        'Change Job Profile': 'Increase',
                                        'Full-Time to Part-Time': 'Move PT',
                                        'Part-Time to Full-Time': 'Move FT',
                                        'Move to another position on my team': 'Increase',
                                        'Move to another company/business unit': 'Lateral',
                                        'Move to another cost center/department': 'Lateral'})

        # ensure outbound datatimes are correct
        data_1['Effective Date'] = pd.to_datetime(data_1['Effective Date'])

        return data_1

    comp = format_new_data(df0)


    def calc_time_deltas(pop, comp):
        """
        calculates days since last promotion and since last increase
        :param df:
        :return:
        """
        # todo: isolate to narrow data sets
        c1 = comp[['ID', 'Effective Date', 'effective_year',
                   'move_type_a', 'move_type_b',
                   'annual_prior', 'annual_post', 'annual_delta',
                   'pct_delta', 'pay_rate_change']]

        pop1 = pop[['ID', 'record_date']]
        pop1['record_date'] = pd.to_datetime(pop1['record_date'])

        # todo: create last promotion (increase, pct, date)
        # isolate to promotion changes only
        promos = c1.loc[c1['move_type_a'] == 'Promotion']

        # relabel promo data for merges
        pr1 = promos[['ID', 'Effective Date', 'effective_year',
                      'pct_delta', 'annual_delta']]
        pr2 = pr1.rename(columns={'Effective Date': 'promotion_date',
                                     'effective_year': 'promotion_year',
                                     'pct_delta': 'promotion_pct_increase',
                                  'annual_delta': 'promotion_dollars'})
        # find most recent event
        #  sort values by most recent date
        #  drop duplicates on the ID field, keeping first entry
        pr3 = pr2.sort_values(by='promotion_date', ascending=False)
        pr4 = pr3.drop_duplicates(subset='ID', keep='first')

        # merge promotion data on narrow population data
        pr5 = pd.merge(pop1, pr4, on='ID', how='left')

        # calculate time distance since the last promotion
        pr5['days_since_last_promo'] = ((pr5['record_date']) - (pr5['promotion_date'])).dt.days


        # todo: create last increase (increase, pct, date)
        increases = c1.loc[c1['move_type_b'].isin(['Promotion', 'Increase', 'Lateral', 'Move FT'])]
        # relabel increase data for merges
        inc1 = increases[['ID', 'Effective Date', 'effective_year',
                          'move_type_b', 'pct_delta', 'annual_delta']]
        inc2 = inc1.rename(columns={'Effective Date': 'increase_date',
                                    'effective_year': 'increase_year',
                                    'pct_delta': 'increase_pct',
                                    'move_type_b': 'increase_type',
                                    'annual_delta': 'increase_dollars'})

        # find most recent increase event
        inc3 = inc2.sort_values(by='increase_date', ascending=False)
        inc4 = inc3.drop_duplicates(subset='ID', keep='first')

        # merge increase data on pop data
        inc5 = pd.merge(pop1, inc4, on='ID', how='left')

        # calculate time delta
        inc5['days_since_last_increase'] = ((inc5['record_date']) - (inc5['increase_date'])).dt.days

        # merge promotion & increase dataframes on to population data
        #  drop record date column
        inc6 = inc5.drop(columns='record_date')
        pr6 = pr5.drop(columns='record_date')

        return inc6, pr6

    increases, promos = calc_time_deltas(pop, comp)



    def format_merit_data(pop, target_date, eids):
        """
        Finds the most recent merit increases in time and size
        :param merits:
        :param pop_with_comp_chg:
        :return:
        """
        # get merit compensation data
        filename = ('C:\\Users\\DuEvans\\Downloads\\comp_changes_' + target_date + '.xlsx')
        merit = pd.read_excel(filename, skiprows=1)

        # temporary!
        #  add EID field to the data
        merit = pd.merge(merit, eids, on='Employee', how='left')
        merit = merit.drop_duplicates()
        # clean the data
        # remove non-completed fields on status column
        merit = merit.loc[merit['Status'] == 'Successfully Completed']

        # filter out hires (just internal moves)
        merit = merit.loc[~merit['Business Process Name'].isin(['Propose Compensation Hire'])]

        # create a year field
        merit['Effective Date'] = pd.to_datetime(merit['Effective Date'])
        merit['year'] = merit['Effective Date'].dt.year

        # annualize all compensation fields
        merit['annual_prior'] = np.where(merit['Frequency'] == 'Hourly', (
                merit['Current Base Pay'] * 2080), merit['Current Base Pay'])

        merit['annual_post'] = np.where(merit['Frequency.1'] == 'Hourly', (
                merit['Proposed Base Pay'] * 2080), merit['Proposed Base Pay'])

        # create compensation delta field
        merit['delta'] = merit['annual_post'] - merit['annual_prior']
        merit['delta'] = merit['delta'].round(decimals=0)
        # trim the dataframe so that outliers on comp delta are moved to 0
        merit.loc[merit.delta > 50000, 'delta'] = 0
        merit.loc[merit.delta < -10000, 'delta'] = 0

        merit = merit.loc[merit['delta'] >= 50]

        # create compensation percent delta field
        merit['pct_delta'] = merit['delta'] / merit['annual_prior']

        # identify promotions
        merit['promotion'] = np.where(merit['Reason'].str.contains('Promotion'), 'Promotion', 'Non-Promotion')

        # remove promotions where the comp delta is 0
        merit.loc[merit.delta == 0, 'promotion'] = 'Non-Promotion'

        # create separate dataframe that removes 0, inf, and nan comp deltas
        merit_data = merit.loc[merit['delta'] != 0]

        merit_data['check_rates'] = np.where(merit_data['Frequency'] == merit_data['Frequency.1'],
                                             'match', 'diff')

        return merit_data

    merit_data = format_merit_data(pop, target_date, eids)

    def find_merit_increases(merit_data):
        """"""
        # create dataframe of merit increases
        df_merit = merit_data.loc[merit_data['Reason'] == 'Request Compensation Change > Base Salary Change > Merit']

        # calculate type of merit increase via pct_delta field
        #  >= .0325 would be exceptional, other is regular
        df_merit['merit_type'] = np.where((df_merit['pct_delta'] >= .0325),
                                    'Exceptional',
                                    'Regular')

        merit_increases = df_merit[['ID', 'Effective Date', 'delta', 'pct_delta', 'year', 'merit_type']]
        all_merits = df_merit[['ID', 'Effective Date', 'delta', 'pct_delta', 'year', 'annual_prior', 'annual_post', 'merit_type']]

        return merit_increases, all_merits

    merit_increases, all_merits = find_merit_increases(merit_data)

    def find_recent_merits(pop, merit_increases):
        """
        find most recent merit increases for FT population.
        :param merit_increases:
        :return:
        """
        merit_increases = merit_increases.rename(columns={'Effective Date': 'date_merit',
                                                      'delta': 'merit_dollar_increase',
                                                      'pct_delta': 'merit_percent_increase',
                                                      'year': 'last_merit_year'})
        pop1 = pop[['ID', 'record_date']]

        # calculate last merit increase
        lmerit = merit_increases.sort_values(by='date_merit', ascending=False)
        lm = pd.merge(pop1, lmerit, on='ID', how='left', indicator=True)
        lm = lm.sort_values(by='date_merit', ascending=False)
        lm = lm.drop_duplicates(subset='ID', keep='first')

        lm['record_date'] = pd.to_datetime(lm['record_date'])

        # date of last merit increase
        lm['days_since_last_merit'] = ((lm['record_date']) - (lm['date_merit'])).dt.days

        # drop unneeded columns
        lm = lm.drop(columns=['record_date', '_merge'])

        return lm

    lmerit = find_recent_merits(pop, merit_increases)

    def add_to_pop_data(pop, promos, increases, merits):
        """
        merges all three types of compensation change data
        to overall population records
        :param pop:
        :param promos:
        :param increases:
        :param merits:
        :return:
        """
        pop2 = pd.merge(pop, promos, on='ID', how='left')
        pop3 = pd.merge(pop2, increases, on='ID', how='left')
        pop4 = pd.merge(pop3, merits, on='ID', how='left')

        return pop4

    comp_pop = add_to_pop_data(pop, promos, increases, lmerit)

    def calc_all_increases(pop, merits, comp):
        """
        calculates the total promotions awarded, and total increases received
        :param df:
        :return:
        """
        # format each dataframe to be appended
        #  format comp increases (non-merit)
        c1 = comp[['ID', 'Effective Date', 'effective_year',
                   'move_type_a', 'move_type_b',
                   'annual_prior', 'annual_post', 'annual_delta',
                   'pct_delta']]

        #  format merit increases
        m1 = all_merits.rename(columns={'year': 'effective_year',
                                             'delta': 'annual_delta'})
        m1['move_type_a'] = 'Merit'
        m1['move_type_b'] = m1['merit_type']

        # append each dataframe
        df1 = pd.concat([c1, m1], sort=False)

        # todo: solution to remove duplicates
        #  see 'aj smith' in documentation

        # drop 'add job', 'move pt' from dataset
        df2 = df1.loc[~df1['move_type_b'].isin(['Add Job', 'Move PT'])]

        # todo: pivot on EID
        df3 = pd.pivot_table(df2, values='Effective Date', index='ID',
                             columns='move_type_b', aggfunc=len, margins=True)

        # create dataframe of EID pivot
        df4 = pd.DataFrame(df3)
        # remove the 'all' row
        df5 = df4[:-1]
        df6 = df5.rename(columns={'All': 'count_increases',
                                  'Exceptional': 'exceptional_merit_increases',
                                  'Increase': 'position_increases',
                                  'Lateral': 'lateral_increases',
                                  'Move FT': 'move_ft_increases',
                                  'Promotion': 'promotion_increases',
                                  'Regular': 'standard_merit_increases'})

        # non-regular increases: count_increases minus count of merit increases
        df6['count_extraordinary'] = df6['count_increases'] - df6['standard_merit_increases']

        # fill na values with 0
        df6 = df6.fillna(0)

        # calculate total percentage of increases and average increases
        total_pct = pd.pivot_table(df2, values='pct_delta', index='ID', aggfunc=(np.sum, np.average))
        tpct_df = pd.DataFrame(total_pct)
        tpct_df = tpct_df.rename(columns={'average': 'average_pct_increase',
                                          'sum': 'cumulative_pct_increase'})

        # todo: merge counts and total percentage increases
        df7 = pd.merge(df6, tpct_df, on='ID', how='left')

        # merge pivoted dataframe on to population records
        pop1 = pop[['ID', 'record_date']]
        pop_all_increases = pd.merge(pop1, df7, on='ID', how='left')
        pop_all_increases = pop_all_increases.drop(columns='record_date')

        return pop_all_increases

    pop_all_increases = calc_all_increases(pop, all_merits, comp)

    def merge_comp_frames(comp_pop, pop_all_increases):
        """merges the recent change df with the all change df"""
        df8 = pd.merge(comp_pop, pop_all_increases, on='ID', how='left')

        return df8

    pop_with_all_comp = merge_comp_frames(comp_pop, pop_all_increases)

    def update_comp_pop_gspread(data):
        """pushes information to a google spreadsheet"""
        client = gcred()
        gs1 = client.open('population_comp_changes').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, data)
        print('\nGspread updated.')

    update_comp_pop_gspread(pop_with_all_comp)
    update_comp_pop_gspread(pop_with_all_comp)

    print('\nProcess complete.')



def push(data):
    """pushes data to a gspread for review."""
    client = gcred()
    gs1 = client.open('temp_comp_change_testing').sheet1
    gs1.clear()
    gspread_dataframe.set_with_dataframe(gs1, data)
    print('Done.')

from get_training_participants import get_training_participants

train = get_training_participants()
pt = train.loc[train['training'] == 'people training']
pop_trained = pd.merge(pop_with_all_comp, pt, on='ID', how='left', indicator=True)
people_trained = pop_trained.loc[pop_trained['_merge'] == 'both']
push(people_trained)
