import pandas as pd
import os
import gspread
import gspread_dataframe
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

os.chdir('C:\\Users\\DuEvans\\Documents\\ktp_data')
pd.options.mode.chained_assignment = None  # default='warn'

print('\nPinging finance records for updated BRM references...')

# pull data from the brm_records google sheet
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# xref_activity sheet
xref_act = client.open("brm_records").worksheet('xref_activity')
xref_act = xref_act.get_all_records()
xref_act = pd.DataFrame.from_records(xref_act)

# mstr_activty sheet
mstr_act = client.open("brm_records").worksheet('mstr_activity')
mstr_act = mstr_act.get_all_records()
mstr_act = pd.DataFrame.from_records(mstr_act)

# mstr_process sheet
mstr_pro = client.open("brm_records").worksheet('mstr_process')
mstr_pro = mstr_pro.get_all_records()
mstr_pro = pd.DataFrame.from_records(mstr_pro)

# mstr_brm sheet (this is the 'category' label in other sheets
mstr_brm = client.open("brm_records").worksheet('mstr_brm')
mstr_brm = mstr_brm.get_all_records()
mstr_brm = pd.DataFrame.from_records(mstr_brm)

# other out of scope roles
custom_adds = client.open('brm_key').worksheet('out_of_scope_adds')
custom_adds = custom_adds.get_all_records()
custom_adds = pd.DataFrame.from_records(custom_adds)

print('\nNew records accessed. Creating key.')

# create a brm key from the brm_records
brm_act = xref_act[['LOCAL_ACTIVITY_ID', 'ACTIVITY_ID']]
brm_act = pd.merge(brm_act, mstr_act, on='ACTIVITY_ID', how='left')

brm_act_pro = pd.merge(brm_act, mstr_pro, on='PROCESS_ID', how='left')

brm_act_pro_brm = pd.merge(brm_act_pro, mstr_brm, on='BRM_ID', how='left')

brm_key = brm_act_pro_brm[['LOCAL_ACTIVITY_ID', 'ACTIVITY_ID', 'ACTIVITY_DESC',
                           'ACTIVITY_SHORT_DESC', 'PROCESS_ID', 'BRM_ID',
                           'PROCESS_DESC', 'PROCESS_SHORT_DESC', 'BRM_DESC']]

# find out of scope roles
custom_adds = client.open('brm_key').worksheet('out_of_scope_adds')
custom_adds = custom_adds.get_all_records()
custom_adds = pd.DataFrame.from_records(custom_adds)

brm_key = brm_key.append(custom_adds, sort=False)

print('\nUpdating people analytics records.')

# store the brm key to the brm_key google sheet
gs1 = client.open('brm_key').worksheet('finance_labels')
gs1.clear()
gspread_dataframe.set_with_dataframe(gs1, brm_key)

# rename the columns to match currently used format
brm_key_labeled = brm_key.rename(columns={'ACTIVITY_SHORT_DESC': 'Activity',
                                          'PROCESS_SHORT_DESC': 'Process',
                                          'BRM_DESC': 'Category'})

# store the readable version to the brm_key google sheet
gs2 = client.open('brm_key').worksheet('custom_labels')
gs2.clear()
gspread_dataframe.set_with_dataframe(gs2, brm_key_labeled)


# update the csv version of the brm_map
brm_key_labeled.to_csv('brm_map.csv', index=False)

print('\nProcess finished.')
