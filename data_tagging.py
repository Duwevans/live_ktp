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

    df_0 = pd.merge(dataset, mgr_key, on='Manager ID', how='left', sort=False)
    # remove nan values from the matched dataset

    mgr_nan = df_0[df_0['Structure'].isnull()]

    # remove the nan values from the original dataset
    mgr_mapped = df_0[df_0['Structure'].notnull()]

    # drop the nan columns
    mgr_nan = mgr_nan.drop(['Primary Key', 'Structure', 'Group', 'Team'], axis=1)

    # match the values to the EEID map
    eeid_mapped = pd.merge(mgr_nan, eeid_key, on='ID', how='left', sort=False)

    # compile the new dataset
    pop_mapped = mgr_mapped.append(eeid_mapped, sort=False)

    pop_mapped = pop_mapped.rename(columns={'Primary Key': 'Manager'})

    # map to Structure B values
    pop_mapped['Structure B'] = pop_mapped['Group'].map({'Admissions Group': 'Admissions', 'Technology': 'Common',
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

    dataset = pop_mapped
    print(list(dataset))
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
    print(list(dataset))
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
    dataset.loc[dataset['ID'] == 'P000025952', 'Gender'] = 'Male'

    return dataset

