import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from datetime import date
import os
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from data_tagging import *
from sqlalchemy import create_engine
from gcred import gcred

pd.options.mode.chained_assignment = None  # default='warn'


def create_pop_data_pairs():
    # loop through each file in the all_ktp records
    pop_file_directory = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp\\'
    # create list to hold date values
    dates = []
    for file in os.listdir(pop_file_directory):
        # find the date value in the filename
        file_d1 = file[:-4]
        file_date = file_d1[12:]
        dates.append(file_date)

    # find available date to match the most recent date
    date_matches = {}
    for date in dates:
        delta = 0
        # go back no farther than 1/1/2018 (12/31/2017) - data not available daily
        if date == '12_31_2017':
            continue
        # find if date has a match in the list of dates
        while True:
            delta += 1
            time_date1 = datetime.strptime(date,'%m_%d_%Y')
            date2 = time_date1 - timedelta(days=delta)
            s_date2 = date2.strftime('%m_%d_%Y')
            if s_date2 in dates:
                date_matches[date] = s_date2
                break
    return date_matches



def condense_pop_data(dataframe):
    """Condenses population data to only the needed information"""
    dataframe = dataframe.rename(columns = {'Preferred Name in General Display Format': 'ee_name',
                                'Management Level A': 'mgt_lvl',
                                'Total Base Pay Annualized - Amount': 'salary'})
    dataframe_condensed = dataframe[['ID', 'ee_name', 'mgt_lvl', 'FT / PT', 'salary', 'record_date',
                                     'Ethnicity', 'Gender', 'Structure B', 'Group', 'Team',
                                     'Age', 'Age Bracket', 'Digital A', 'Digital B',
                                     'Prepare/New A', 'Prepare/New B', 'yrs_tenure',
                                     'days_tenure']]

    return dataframe_condensed

def format_pop_data(dataframe):
    """formats the population dataframes"""

    # create numeric variable from mgt_lvl field
    dataframe['lvl'] = dataframe['mgt_lvl'].map({'Individual Contributor': 0,
                                                 'Manager': 1,
                                                 'Director': 2,
                                                 'Executive Director': 3,
                                                 'VP': 4,
                                                 'Above VP': 5})

    # create numeric variable from time_type field
    dataframe['time_type'] = dataframe['FT / PT'].map({'Part time': 0,
                                                       'Full time': 1})

    return dataframe

def analyze_date_pairs(date_matches):
    """"""
    # create two dataframes, format, and combine
    # a is more recent date; b is previous date
    # create
    df_changes = pd.DataFrame()
    for a, b in date_matches.items():
        print(a + ': ' + b)
        pop_path = 'C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\all_ktp\\ktp_all_pop_'
        path_a = (pop_path + a + '.csv')
        path_b = (pop_path + b + '.csv')
        dfa = pd.read_csv(path_a)
        dfb = pd.read_csv(path_b)

        dfa = condense_pop_data(dfa)
        dfb = condense_pop_data(dfb)

        # todo: format
        dfa = format_pop_data(dfa)
        dfb = format_pop_data(dfb)

        # set ID as index
        dfa = dfa.set_index('ID')
        dfb = dfb.set_index('ID')

        # add suffix to fields
        dfa = dfa.add_suffix('_a')
        dfb = dfb.add_suffix('_b')

        # todo: combine
        df = pd.merge(dfa, dfb, how='outer', on='ID', indicator=True)
        # note: right_only values mean only present in the new data set
        # note: left_only values mean only present in the previous data set

        # todo: re-code new variables

        df['lvl_chg'] = df['lvl_a'] - df['lvl_b']
        df['lvl_chg'] = df['lvl_chg'].fillna(0)
        df['time_chg'] = df['time_type_a'] - df['time_type_b']

        # note: no value in field a means entry is a term
        # note: no value in field b means entry is a hire
        df['hires'] = np.where(df['lvl_b'].isnull(), 1, 0)
        df['terms'] = np.where(df['lvl_a'].isnull(), 1, 0)

        # compensation changes
        df['salary_chg'] = df['salary_a'] - df['salary_b']

        # todo: assign date of change to the database (new date)
        date_a = datetime.strptime(a,'%m_%d_%Y')
        df['date_of_change'] = date_a

        # todo: append change dataframe
        #df_all = df_all.append(df)

        # todo: isolate to only rows with changes
        hires = df.loc[df['hires'] == 1]
        hires['reason'] = 'hire'
        terms = df.loc[df['terms'] == 1]
        terms['reason'] = 'term'
        to_ft = df.loc[df['time_chg'] == 1]
        to_ft['reason'] = 'to_ft'
        to_pt = df.loc[df['time_chg'] == -1]
        to_pt['reason'] = 'to_pt'
        lvl_chg = df.loc[df['lvl_chg'] != 0]
        lvl_chg['reason'] = 'lvl'

        # combine to change-only dataframe
        df1 = pd.DataFrame()
        df1 = df1.append(hires)
        df1 = df1.append(terms)
        df1 = df1.append(to_ft)
        df1 = df1.append(to_pt)
        df1 = df1.append(lvl_chg)

        # append change dataframe
        df_changes = df_changes.append(df1)

    # todo: isolate to only rows with changes
    df_changes['id'] = df_changes.index

    # todo: find only rows where either value is full time
    df_changes['ft_once'] = np.where(((df_changes['time_type_a'] == 1) | (df_changes['time_type_b'] == 1)),
                          1, 0)

    def create_features(df_changes):
        """takes a and b fields and creates a single field,
        using the newer value where possible"""
        df_changes['ee_name'] = np.where(pd.isnull(df_changes.ee_name_a),
                                         df_changes.ee_name_b,
                                         df_changes.ee_name_a)
        df_changes['mgt_lvl'] = np.where(pd.isnull(df_changes.mgt_lvl_a),
                                         df_changes.mgt_lvl_b,
                                         df_changes.mgt_lvl_a)
        df_changes['time_type'] = np.where(pd.isnull(df_changes.time_type_a),
                                         df_changes.time_type_b,
                                         df_changes.time_type_a)
        df_changes['salary'] = np.where(pd.isnull(df_changes.salary_a),
                                         df_changes.salary_b,
                                         df_changes.salary_a)
        df_changes['ethnicity'] = np.where(pd.isnull(df_changes.Ethnicity_a),
                                         df_changes.Ethnicity_b,
                                         df_changes.Ethnicity_a)
        df_changes['gender'] = np.where(pd.isnull(df_changes.Gender_a),
                                         df_changes.Gender_b,
                                         df_changes.Gender_a)
        df_changes['structure'] = np.where(pd.isnull(df_changes['Structure B_a']),
                                         df_changes['Structure B_b'],
                                         df_changes['Structure B_a'])
        df_changes['group'] = np.where(pd.isnull(df_changes['Group_a']),
                                         df_changes['Group_b'],
                                         df_changes['Group_a'])
        df_changes['team'] = np.where(pd.isnull(df_changes['Team_a']),
                                         df_changes['Team_b'],
                                         df_changes['Team_a'])
        df_changes['age'] = np.where(pd.isnull(df_changes['Age_a']),
                                         df_changes['Age_b'],
                                         df_changes['Age_a'])
        df_changes['age_bracket'] = np.where(pd.isnull(df_changes['Age Bracket_a']),
                                         df_changes['Age Bracket_b'],
                                         df_changes['Age Bracket_a'])
        df_changes['digital_a'] = np.where(pd.isnull(df_changes['Digital A_a']),
                                         df_changes['Digital A_b'],
                                         df_changes['Digital A_a'])
        df_changes['digital_b'] = np.where(pd.isnull(df_changes['Digital B_a']),
                                         df_changes['Digital B_b'],
                                         df_changes['Digital B_a'])
        df_changes['prepare_new_a'] = np.where(pd.isnull(df_changes['Prepare/New A_a']),
                                         df_changes['Prepare/New A_b'],
                                         df_changes['Prepare/New A_a'])
        df_changes['prepare_new_b'] = np.where(pd.isnull(df_changes['Prepare/New B_a']),
                                         df_changes['Prepare/New B_b'],
                                         df_changes['Prepare/New B_a'])
        df_changes['time_type_text'] = np.where(df_changes['time_type'] == 1,
                                                'full_time',
                                                'part_time')
        df_changes['yrs_tenure'] = np.where(pd.isnull(df_changes['yrs_tenure_a']),
                                         df_changes['yrs_tenure_b'],
                                         df_changes['yrs_tenure_a'])
        df_changes['days_tenure'] = np.where(pd.isnull(df_changes['days_tenure_a']),
                                         df_changes['days_tenure_b'],
                                         df_changes['days_tenure_a'])
        return df_changes

    df_changes = create_features(df_changes)

    # trim to just prepare
    df_changes = df_changes.loc[df_changes['structure'].isin(['Admissions',
                                                              'Licensure',
                                                              'Common',
                                                              'NXT'])]

    # trim to remove non-hire entries where tenure is =< 31 days
    df_hires = df_changes.loc[df_changes['reason'] == 'hire']
    df_other = df_changes.loc[~df_changes['reason'].isin(['hire'])]
    df_other = df_other.loc[df_other['days_tenure'] >= 31]
    # combine
    df_changes = df_other.append(df_hires)

    df_ft = df_changes.loc[df_changes['ft_once'] == 1]



    df_dir_plus = df_ft.loc[df_ft['mgt_lvl_a'].isin(['Director', 'VP',
                                                     'Executive Director',
                                                     'Above VP'])]



    df_dir_plus = df_dir_plus.loc[df_dir_plus['reason'].isin(['hire', 'lvl'])]
    df_dir_plus = df_dir_plus.sort_values(by=['date_of_change'], ascending=False)
    df_dir_plus['id'] = df_dir_plus.index
    client = gcred()

    # update director plus spreadsheet
    gs1 = client.open('director_plus_moves_test').sheet1
    gs1.clear()
    gspread_dataframe.set_with_dataframe(gs1, df_dir_plus)

    # update all moves spreadsheet
    df_ft = df_ft.sort_values(by=['date_of_change'], ascending=False)
    gs2 = client.open('all_ft_pop_moves').sheet1
    gs2.clear()
    gspread_dataframe.set_with_dataframe(gs2, df_ft)

    # update the previous n sheet





    # todo: previous 15 sheet
    dir_plus_prev_15 = df_dir_plus.head(15)
    gs3 = client.open('[for website] last 15 (Director +)').worksheet('prev_15')
    gs3.clear()
    gspread_dataframe.set_with_dataframe(gs3, dir_plus_prev_15)

    # todo: previous 30 sheet
    dir_plus_prev_30 = df_dir_plus.head(30)
    gs4 = client.open('[for website] last 15 (Director +)').worksheet('prev_30')
    gs4.clear()
    gspread_dataframe.set_with_dataframe(gs4, dir_plus_prev_30)

    # todo: ytd sheet
    dir_plus_ytd = df_dir_plus.loc[df_dir_plus['date_of_change'] > '12/31/2018']
    gs5 = client.open('[for website] last 15 (Director +)').worksheet('ytd')
    gs5.clear()
    gspread_dataframe.set_with_dataframe(gs5, dir_plus_ytd)

    # todo: since 2018 sheet
    dir_plus_since_2018 = df_dir_plus.loc[df_dir_plus['date_of_change'] > '12/31/2017']
    gs6 = client.open('[for website] last 15 (Director +)').worksheet('since_2018')
    gs6.clear()
    gspread_dataframe.set_with_dataframe(gs6, dir_plus_since_2018)


    return df_ft


# these are the functions that run
date_matches = create_pop_data_pairs()
all_ft_chg = analyze_date_pairs(date_matches)

