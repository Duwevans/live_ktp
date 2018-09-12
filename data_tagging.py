import pandas as pd


pd.options.mode.chained_assignment = None  # default='warn'

def manager_map(dataset):
    """
    Maps the data set with the manager key - Structure, Structure B, Group, Team
    :param dataset:
    :return:
    """
    # read the manager key
    mgr_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\meid_key.csv')
    # read the eid key
    eeid_key = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\mgr_key\\eid_key.csv')
    # format the eid key to match the manager map
    eeid_key.columns = ['Name', 'ID', 'Structure', 'Group', 'Team']

    dataset = pd.merge(dataset, mgr_key, on='Manager ID', how='left', sort=False)
    # remove nan values from the matched dataset

    mgr_nan = dataset[dataset['Structure'].isnull()]

    # remove the nan values from the original dataset
    mgr_mapped = dataset[dataset['Structure'].notnull()]

    # drop the nan columns
    mgr_nan = mgr_nan.drop(['Primary Key', 'Structure', 'Group', 'Team'], axis=1)

    # match the values to the EEID map
    eeid_mapped = pd.merge(mgr_nan, eeid_key, on='ID', how='left', sort=False)

    # compile the new dataset
    pop_mapped = mgr_mapped.append(eeid_mapped, sort=False)

    dataset = pop_mapped.rename(columns={'Primary Key': 'Manager'})

    # map to Structure B values
    dataset['Structure B'] = dataset['Group'].map({'Admissions Group': 'Admissions', 'Technology': 'Common',
                                                         'NXT': 'NXT', 'Licensure Group': 'Licensure',
                                                         'Med': 'Licensure', 'Finance & Accounting': 'Common',
                                                         'Admissions Faculty': 'Admissions', 'Nursing': 'Licensure',
                                                         'MPrep': 'Admissions', 'Marketing': 'Common',
                                                         'Bar': 'Licensure',
                                                         'HR / PR / Admin': 'Common', 'Publishing': 'Common',
                                                         'Data and Learning Science': 'Common', 'Metis': 'New Ventures',
                                                         'Digital Media': 'Common', 'iHuman': 'New Ventures',
                                                         'Advise': 'Advise', 'International': 'Licensure',
                                                         'Metis Faculty': 'New Ventures',
                                                         'Admissions Core': 'Admissions',
                                                         'Admissions New': 'Admissions', 'Allied Health': 'Licensure',
                                                         'Legal': 'Common', 'TTL Labs': 'New Ventures',
                                                         'Executive': 'Common',
                                                         'Licensure Programs': 'Licensure'})
    # adjust JP
    dataset.loc[dataset['ID'] == 'P000025952', 'Structure'] = 'Common'
    dataset.loc[dataset['ID'] == 'P000025952', 'Team'] = 'CEO'


    return dataset

def prepare_or_new(dataset):
    """
    Maps the data set to either prepare/new A and prepare/new B
    :param dataset:
    :return:
    """
    # this is just either 'Prepare,' or 'New'
    dataset['Prepare/New A'] = dataset['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
                                                           'NXT': 'Prepare', 'Licensure Group': 'Prepare',
                                                           'Med': 'Prepare', 'Finance & Accounting': 'Prepare',
                                                           'Admissions Faculty': 'Prepare', 'Nursing': 'Prepare',
                                                           'MPrep': 'Prepare', 'Marketing': 'Prepare', 'Bar': 'Prepare',
                                                           'HR / PR / Admin': 'Prepare', 'Publishing': 'Prepare',
                                                           'Data and Learning Science': 'Prepare', 'Metis': 'New',
                                                           'Digital Media': 'Prepare', 'iHuman': 'New',
                                                           'Advise': 'New', 'International': 'Prepare',
                                                           'Metis Faculty': 'New', 'Admissions Core': 'Prepare',
                                                           'Admissions New': 'Prepare', 'Allied Health': 'Prepare',
                                                           'Legal': 'Prepare', 'DBC/TTL': 'New', 'Executive': 'Prepare',
                                                           'Licensure Programs': 'Prepare'})

    # this is either 'Prepare,' or the specific new business group
    dataset['Prepare/New B'] = dataset['Group'].map({'Admissions Group': 'Prepare', 'Technology': 'Prepare',
                                                           'NXT': 'Prepare', 'Licensure Group': 'Prepare',
                                                           'Med': 'Prepare', 'Finance & Accounting': 'Prepare',
                                                           'Admissions Faculty': 'Prepare', 'Nursing': 'Prepare',
                                                           'MPrep': 'Prepare', 'Marketing': 'Prepare', 'Bar': 'Prepare',
                                                           'HR / PR / Admin': 'Prepare', 'Publishing': 'Prepare',
                                                           'Data and Learning Science': 'Prepare', 'Metis': 'Metis',
                                                           'Digital Media': 'Prepare', 'iHuman': 'iHuman',
                                                           'Advise': 'Advise', 'International': 'Prepare',
                                                           'Metis Faculty': 'Metis', 'Admissions Core': 'Prepare',
                                                           'Admissions New': 'Prepare', 'Allied Health': 'Prepare',
                                                           'Legal': 'Prepare', 'DBC/TTL': 'DBC/TTL',
                                                           'Executive': 'Prepare',
                                                           'Licensure Programs': 'Prepare'})
    return dataset


def digital_map(dataset):
    """
    Maps the data set to either digital or blank, and the digital subcategory
    :param dataset:
    :return:
    """
    # label everything into current digital/technology/marketing roles

    dataset['Digital A'] = dataset['Team'].map({'Analytics and Digital Marketing': 'Marketing',
                                                    'Email Marketing': 'Marketing', 'Growth': 'Marketing',
                                                    'Market Research': 'Marketing', 'Marketing Leadership': 'Marketing',
                                                    'Cloud Operations': 'Technology', 'Data Engineering': 'Technology',
                                                    'Delivery Management': 'Technology',
                                                    'MPrep Technology': 'Technology',
                                                    'Platform': 'Technology', 'UX': 'Technology',
                                                    'Website': 'Technology',
                                                    'Data Science': 'Data Analytics',
                                                    'Learning Science': 'Data Analytics',
                                                    'Psychometrics': 'Data Analytics',
                                                    'Technology Leadership': 'Technology'})

    dataset['Digital B'] = dataset['Team'].map({'Analytics and Digital Marketing': 'Digital',
                                                      'Email Marketing': 'Digital', 'Growth': 'Digital',
                                                      'Market Research': 'Digital', 'Marketing Leadership': 'Digital',
                                                      'Cloud Operations': 'Digital', 'Data Engineering': 'Digital',
                                                      'Delivery Management': 'Digital', 'MPrep Technology': 'Digital',
                                                      'Platform': 'Digital', 'UX': 'Digital', 'Website': 'Digital',
                                                      'Data Science': 'Digital', 'Learning Science': 'Digital',
                                                      'Psychometrics': 'Digital', 'Technology Leadership': 'Digital'})

    # fill null values with 'non-digital'
    null_value = 'Non Digital'
    dataset['Digital A'] = dataset['Digital A'].fillna(null_value)
    dataset['Digital B'] = dataset['Digital B'].fillna(null_value)

    return dataset

def brm_map(dataset):
    """
    Maps the data set against BRM category, process, activity
    :param dataset:
    :return:
    """
    # create the needed field to map to BRM

    dataset['_1'] = dataset['Cost Centers'].str[:6]
    dataset['_2'] = dataset['_1'].str[:2]
    dataset['_3'] = dataset['_1'].str[-4:]
    dataset['_4'] = (dataset['_2'] + "_" + dataset['_3'])
    dataset['brm_key'] = (dataset['Single Job Family'] + "_" + dataset['_4'])
    dataset['brm_key'] = dataset['brm_key'].str.lower()
    dataset = dataset.drop(columns=['_1', '_2', '_3', '_4'])

    # read the amend the BRM file

    brm_map = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\brm_map.csv', encoding='latin1')
    brm_map['brm_key'] = brm_map['LOCAL_ACTIVITY_ID']
    brm_map['brm_key'] = brm_map['brm_key'].str.lower()
    brm_map = brm_map[['brm_key', 'Activity', 'Process', 'Category']]

    # merge the two files w/ BRM categories

    dataset = pd.merge(dataset, brm_map, on='brm_key', how='left', sort=False)

    return dataset

def map_ethnicity(dataset):
    """
    Maps ethnicities into usable variables.
    :param dataset:
    :return:
    """
    dataset['Ethnicity'] = dataset['Race/Ethnicity (Locale Sensitive)'].map(
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
    dni_value = 'Did not identify'
    dataset['Ethnicity'] = dataset['Ethnicity'].fillna(value=dni_value)

    dataset.loc[dataset['ID'] == 'P000025952', 'Gender'] = 'Male'

    return dataset

def age_brackets(dataset):
    """
    Maps age brackets into buckets.
    :param dataset:
    :return:
    """
    # bin age into age ranges
    age_bin_names = ['18 to 24', '25 to 34', '35 to 44', '45 to 54', '55 to 64', '65+']
    age_bins = [18, 24, 34, 44, 54, 64, 100]
    dataset['Age Bracket'] = pd.cut(dataset['Age'], age_bins, labels=age_bin_names)

    return dataset

def management_levels(dataset):
    """
    Maps management levels into more readable labels.
    Keeps a few key outdated individuals updated.
    :param dataset:
    :return:
    """
    dataset['Management Level A'] = dataset['Management Level'].map(
        {'11 Individual Contributor': 'Individual Contributor',
         '9 Manager': 'Manager', '8 Senior Manager': 'Manager',
         '10 Supervisor': 'Manager', '7 Director': 'Director',
         '6 Exec & Sr. Director/Dean': 'Executive Director',
         '5 VP': 'VP', '4 Senior VP': 'Above VP',
         '2 Senior Officer': 'Above VP',
         '3 Executive VP': 'Above VP'})

    # manually change a couple folks to be in the right labels:
    dataset.loc[dataset['ID'] == 'P000238419', 'Management Level A'] = 'VP'
    dataset.loc[dataset['ID'] == 'P000018502', 'Management Level A'] = 'VP'
    dataset.loc[dataset['ID'] == 'P000055603', 'Management Level A'] = 'VP'

    dataset['di_leader'] = dataset['Management Level A'].map({'Individual Contributor': 'n_leader',
                                                    'Manager': 'leader', 'Director': 'leader',
                                                    'Executive Director': 'sr_leader', 'VP': 'sr_leader',
                                                    'Above VP': 'sr_leader'})

    try:
        dataset['di_poc'] = dataset['Ethnicity'].map({'White': 'n_poc', 'Black': 'poc', 'Hispanic': 'poc',
                                                        'Asian': 'poc', 'Two or more': 'poc', 'American Indian': 'poc',
                                                        'Pacific Islander': 'poc'})

    except KeyError as e:
        print('\nError on: ' + e)
        print('\nAdd ethnicity labels before management levels.')
    return dataset

def hierarchy_id_match(dataset):
    """
    Takes a population dataset and maps the employee ID across the management hierarchy fields.
    Saves the updated file to records.
    :param dataset:
    :param filename:
    :return:
    """
    #pop = pd.read_csv('C:\\Users\\DuEvans\\Documents\\ktp_data\\population\\ft_ktp\\ktp_pop_08_13_2018.csv')
    # extract the entire pattern

    dataset['hierarchy_lvl_1'] = dataset['supervisory org - lvl 1'].str.split('(').str.get(1)
    # strip the ')' on the right

    dataset['hierarchy_lvl_1'] = dataset['hierarchy_lvl_1'].str[:-1]

    dataset['hierarchy_lvl_2'] = dataset['supervisory org - lvl 2'].str.split('(').str.get(1)
    dataset['hierarchy_lvl_2'] = dataset['hierarchy_lvl_2'].str[:-1]

    dataset['hierarchy_lvl_3'] = dataset['supervisory org - lvl 3'].str.split('(').str.get(1)
    dataset['hierarchy_lvl_3'] = dataset['hierarchy_lvl_3'].str[:-1]

    # match employee IDs to the name of the manager hierarchy
    eids = dataset[['ID', 'Preferred Name in General Display Format']]
    eids = eids.rename(columns={'Preferred Name in General Display Format': 'name'})
    # manager hierarchy 1
    hier_1_eids = dataset[['hierarchy_lvl_1']]
    hier_1_eids = hier_1_eids.rename(columns={'hierarchy_lvl_1': 'name'})
    # this matches each hierarchy name with their own employee id
    hier_1_eids = pd.merge(hier_1_eids, eids, on='name', how='left')
    # format to match the merge back into the full population
    hier_1_eids = hier_1_eids.rename(columns={'name': 'hierarchy_lvl_1', 'ID': 'hierarchy_lvl_1_id'})
    # drop duplicate entries
    hier_1_eids = hier_1_eids.drop_duplicates(subset='hierarchy_lvl_1')


    # manager hierarchy 2
    hier_2_eids = dataset[['hierarchy_lvl_2']]
    hier_2_eids = hier_2_eids.rename(columns={'hierarchy_lvl_2': 'name'})
    hier_2_eids = pd.merge(hier_2_eids, eids, on='name', how='left')
    hier_2_eids = hier_2_eids.rename(columns={'name': 'hierarchy_lvl_2', 'ID': 'hierarchy_lvl_2_id'})
    hier_2_eids = hier_2_eids.drop_duplicates(subset='hierarchy_lvl_2')

    # manager hierarchy 3
    hier_3_eids = dataset[['hierarchy_lvl_3']]
    hier_3_eids = hier_3_eids.rename(columns={'hierarchy_lvl_3': 'name'})
    hier_3_eids = pd.merge(hier_3_eids, eids, on='name', how='left')
    hier_3_eids = hier_3_eids.rename(columns={'name': 'hierarchy_lvl_3', 'ID': 'hierarchy_lvl_3_id'})
    hier_3_eids = hier_3_eids.drop_duplicates(subset='hierarchy_lvl_3')

    # merge hierarchies 1, 2, and 3 back into the full population
    dataset = pd.merge(dataset, hier_1_eids, on='hierarchy_lvl_1', how='left')
    dataset = pd.merge(dataset, hier_2_eids, on='hierarchy_lvl_2', how='left')
    dataset = pd.merge(dataset, hier_3_eids, on='hierarchy_lvl_3', how='left')

    return dataset

def map_tenure_fields(dataset, records_date):
    """
    adds days & years tenure, and adds tenure brackets to the data set.
    :param dataset:
    :return:
    """

    dataset['days_tenure'] = (records_date - (dataset['(Most Recent) Hire Date'])).dt.days
    dataset['yrs_tenure'] = (dataset['days_tenure'] / 365).round(0)
    dataset['months_tenure'] = (dataset['yrs_tenure'] / 12).round(0)

    yrs_ten_bins = [-1, 2, 4, 6, 8, 10, 100]
    yrs_bin_names = ['0 to 2', '2 to 4', '4 to 6', '6 to 8', '8 to 10', '10+']
    dataset['yrs_tenure_group'] = pd.cut(dataset['yrs_tenure'], yrs_ten_bins, labels=yrs_bin_names)

    return dataset

def map_age_fields(dataset, records_date):
    """
    adds days & years of age, and adds age brackets to the data set.
    :param dataset:
    :return:
    """

    dataset['days_old'] = (records_date - (dataset['Date of Birth (Locale Sensitive)'])).dt.days

    dataset['Age'] = (dataset['days_old'] / 365).round(0)

    # bin age into age ranges
    age_bin_names = ['18 to 24', '25 to 34', '35 to 44', '45 to 54', '55 to 64', '65+']
    age_bins = [18, 24, 34, 44, 54, 64, 100]
    dataset['Age Bracket'] = pd.cut(dataset['Age'], age_bins, labels=age_bin_names)

    return dataset
