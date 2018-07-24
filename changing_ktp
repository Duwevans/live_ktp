import pandas as pd
import numpy as np

pop_1 = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records\\ktp_raw_pop_07_23_2018.csv', index_col=['ID'], encoding='latin1')
pop_0 = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\raw_records\\ktp_raw_pop_05_30_2018.csv', index_col=['ID'], encoding='latin1')

# todo: find the current file

# todo: find the previous file


p1 = pop_1[['FT / PT', 'Scheduled Weekly Hours', 'Preferred Name in General Display Format',
            'Total Base Pay Annualized - Amount', 'Job Profile (Primary)',
            'Management Level']]
p0 = pop_0[['FT / PT', 'Scheduled Weekly Hours', 'Preferred Name in General Display Format',
            'Total Base Pay Annualized - Amount', 'Job Profile (Primary)',
            'Management Level']]

# label the column names as new or previous

p1 = p1.add_suffix(' - New')
p0 = p0.add_suffix(' - Previous')

# add columns from both data frames on EID as index
p2 = pd.merge(p1, p0, on=['ID'], how='left')

# map management level

p2['lvl_new'] = p2['Management Level - New'].map({'11 Individual Contributor': 0,
                                            '10 Supervisor': 1,
                                            '9 Manager ': 1,
                                            '8 Senior Manager': 1,
                                            '7 Director': 2,
                                            '6 Exec & Sr. Director/Dean': 3,
                                            '5 VP': 4,
                                            '4 Senior VP': 5,
                                            '3 Executive VP': 6,
                                            '2 Senior Officer': 7})

p2['lvl_previous'] = p2['Management Level - Previous'].map({'11 Individual Contributor': 0,
                                            '10 Supervisor': 1,
                                            '9 Manager ': 1,
                                            '8 Senior Manager': 1,
                                            '7 Director': 2,
                                            '6 Exec & Sr. Director/Dean': 3,
                                            '5 VP': 4,
                                            '4 Senior VP': 5,
                                            '3 Executive VP': 6,
                                            '2 Senior Officer': 7})

# todo: find those that were hired/termed
# below is temporary fix - only those who appear on both records
p2 = p2.dropna()

# identify change/no change in the four target fields

p2['time_new'] = p2['FT / PT - New'].map({'Full time': 1, 'Part time': 0})
p2['time_old'] = p2['FT / PT - Previous'].map({'Full time': 1, 'Part time': 0})

# rename to make the columns usable
p2 = p2.rename(columns={'Total Base Pay Annualized - Amount - New': 'base_new',
                        'Total Base Pay Annualized - Amount - Previous': 'base_previous',
                        'Job Profile (Primary) - New': 'job_new',
                        'Job Profile (Primary) - Previous': 'job_previous',
                        'Management Level - New': 'level_new',
                        'Management Level - Previous': 'level_previous',
                        'Scheduled Weekly Hours - New': 'hours_new',
                        'Scheduled Weekly Hours - Previous': 'hours_previous'})


# find change in time type
p2['time_chg'] = p2['time_new'] - p2['time_old']

# find change in base pay
p2['base_chg'] = p2['base_new'] - p2['base_previous']

# find change in weekly hours
p2['hours_chg'] = p2['hours_new'] - p2['hours_previous']

# find change in management level
p2['lvl_chg'] = p2['lvl_new'] - p2['lvl_previous']


# todo: isolate just the rows with changes


# todo: save the changes dataframe indexed by date


# todo: append the existing change dataframe with new data
