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
from sqlalchemy import create_engine
import time
import requests
import json
pd.options.mode.chained_assignment = None  # default='warn'



def update_comp_dash(data):
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

        # send to sheet
        gs1 = client.open('calc_comp_all_ktp').sheet1
        gs1.clear()
        gspread_dataframe.set_with_dataframe(gs1, data)

    # run function
    kips = get_kip_data()

    new_data = merge_kip_data(data, kips)

    update_comp_gspread(new_data)

    print('\nComp dashboard updated.')

