import pandas as pd
import numpy as np
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
import os
from oauth2client.service_account import ServiceAccountCredentials
from gspread_pandas import Spread, Client
from gcred import gcred
pd.options.mode.chained_assignment = None  # default='warn'

def get_training_participants():
    client = gcred()
    trained = client.open('training_data').sheet1
    trained = trained.get_all_records()
    trained = pd.DataFrame.from_records(trained)

    return trained

