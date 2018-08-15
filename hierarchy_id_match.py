import pandas as pd

def hierarchy_id_match(dataset, filename):
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
    hier_3_eids = pop[['hierarchy_lvl_3']]
    hier_3_eids = hier_3_eids.rename(columns={'hierarchy_lvl_3': 'name'})
    hier_3_eids = pd.merge(hier_3_eids, eids, on='name', how='left')
    hier_3_eids = hier_3_eids.rename(columns={'name': 'hierarchy_lvl_3', 'ID': 'hierarchy_lvl_3_id'})
    hier_3_eids = hier_3_eids.drop_duplicates(subset='hierarchy_lvl_3')

    # merge hierarchies 1, 2, and 3 back into the full population
    dataset = pd.merge(dataset, hier_1_eids, on='hierarchy_lvl_1', how='left')
    dataset = pd.merge(dataset, hier_2_eids, on='hierarchy_lvl_2', how='left')
    dataset = pd.merge(dataset, hier_3_eids, on='hierarchy_lvl_3', how='left')

    dataset.to_csv(filename, index=False)

