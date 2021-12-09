"""
Olympic Athletes Data Wrangling

Date: December 8th, 2021
Authors: Jay Swint & Cory Little

We want to find:
- who the athletes are that competed in both olympics
- athletes that competed in both but changed countries
"""

import pandas as pd
from nameparser import HumanName
from tabulate import tabulate
from thefuzz import fuzz, process

# creating the dataframes
df_2016 = pd.read_csv('2016_olympics/athletes.csv')
df_2020 = pd.read_csv('2020_olympics/athletes.csv')


# need to create a function that aggregates the 2020 events
def match(event):

    print('waiting')
    if event.lower() == 'water polo' or event.lower() == 'swimming':
        return 'aquatics'

    elif event.lower() == 'sailing':
        return 'sailing'

    else:
        return process.extractOne(event, df_2016['sport'], scorer=fuzz.ratio)[0]


# uses humanName parser to return FIRST Name (or the corresponding position)
def get_first(name):
    # return an empty string for the first name if name > 3
    words = name.split()
    if len(words) > 3:
        return ''

    else:
        name = HumanName(name)
        return name.first


# uses humanName parser to return LAST Name (or the corresponding position)
def get_last(name):
    # return  the name string in alphabetical order for the first name if name > 3
    words = name.split()
    if len(words) > 3:
        words.sort()

        sorted_name = ''
        for word in words:
            sorted_name = sorted_name + word + ' '
        return sorted_name.strip()

    else:
        name = HumanName(name)
        return name.last


# resolves some formatting issues; special case for 2020
def get_first_2020(name):
    # if the length of the name is 3 then it is in last, first, middle format
    words = name.split()
    if len(words) == 3:
        # first name is in the 1 position
        return words[1]

    # return an empty string for the first name if name > 3
    elif len(words) > 3:
        return ''

    else:
        name = HumanName(name)
        return name.last


# resolves some formatting issues; special case for 2020
def get_last_2020(name):
    # return  the name string in alphabetical order for the first name if name > 3
    words = name.split()
    if len(words) > 3:
        words.sort()

        sorted_name = ''
        for word in words:
            sorted_name = sorted_name + word + ' '
        return sorted_name.strip()

    else:
        name = HumanName(name)
        return name.first


def display_pretty_df(df1, df2, select_2020, excluded_df):
    # Setting the display options
    pd.set_option('display.max_rows', 5)
    pd.set_option('display.colheader_justify', 'center')
    # prints table of select rows for merge on name
    print(tabulate(df1.loc[[8, 2622, 11295, 4571, 2612, 10462, 7815], ['name', 'first', 'last', 'nationality', 'sex', 'sport']], headers='keys', tablefmt='fancy_grid'))
    print(len(df1))

    # prints table of select rows for merge on name and country
    print(tabulate(df2.loc[[1164, 9321, 7191, 2032, 1242, 931, 3523], ['name', 'first', 'last', 'country_code', 'gender', 'discipline']], headers='keys', tablefmt='fancy_grid'))
    print(len(df2))

    # prints table of select rows for merge on name, country, and matched events
    # (uses select_2020 df so less rows are processed)
    print(tabulate(select_2020.loc[[1164, 9321, 7191, 2032, 1242, 931, 3523], ['name', 'country_code', 'gender', 'matched_events']], headers='keys', tablefmt='fancy_grid'))

# renames columns and prints table for athletes who changed sports (includes their events)
# takes very long to run --> Need to work on optimizing
    # excluded_df.rename(columns={'nationality_x': '2016_country'}, inplace = True)
    # excluded_df.rename(columns={'country_code_x': '2020_country'}, inplace = True)
    # print(tabulate(excluded_df.loc[5:, ['first', 'last', '2016_country', '2020_country']], headers='keys', tablefmt='fancy_grid'))


def create_name(df):
    df['first'] = df.name.str.lower().apply(get_first)
    df['last'] = df.name.str.lower().apply(get_last)


def create_name_2020(df):
    df['first'] = df.name.str.lower().apply(get_first_2020)
    df['last'] = df.name.str.lower().apply(get_last_2020)


def go(df1, df2):
    #  [2016] adding first and last name columns
    create_name(df1)

    # [2020] adding first and last name columns
    create_name_2020(df2)

    #  ---------------------------- Merging 2016 and 2020 on first_name and last_name columns --------------------------
    matched_names = pd.merge(df1, df2, left_on=['first', 'last'],right_on=['first', 'last'], how='inner').loc[:, ['first', 'last', 'name_x', 'name_y',  'nationality', 'country_code']]
    print(matched_names)
    print(len(matched_names))

    # -------------- Merging 2016 and 2020 on first_name, last_name, and country_code/nationality columns  -------------
    matched_names_country = pd.merge(df1, df2, left_on=['first', 'last', 'nationality'], right_on=['first', 'last', 'country_code'], how='inner').loc[:, ['first', 'last', 'name_x', 'name_y', 'nationality', 'country_code']]
    print(matched_names_country)
    print(len(matched_names_country))

    # ----- aggregation of events ------
    # print(aggregate_events(df2, matched_names_country))
    # df2['discipline'] = df2['discipline'].astype(str)
    # df2['matched_events'] = df2['discipline'].apply(match)

# testing testing testing
    df2.loc[:, ['name', 'first', 'last', 'discipline', 'country_code', 'gender']].to_csv('new_matched_events')
    print('done')

    # ----------------- outputting first and last name to csv for 2016 and 2020 ---------------------------------------
    df1.loc[:, ['first', 'last', 'nationality']].to_csv('2016_first_last')
    df2.loc[:, ['first', 'last', 'country_code']].to_csv('2020_first_last')

    # -------- reformatting the events into a new dataframe ----------
    # quicker to run...
    select_2020 = df2.loc[[1164, 9321, 7191, 2032, 1242, 931, 3523], ['name', 'first', 'last', 'discipline', 'country_code', 'gender']]
    select_2020['matched_events'] = select_2020['discipline'].apply(match)

    # ------------ Merging to find the athletes that are in matched_names but not in matched_names_country ------------
    df_all = matched_names.merge(matched_names_country.drop_duplicates(), left_on=['first', 'last', 'nationality'], right_on=['first', 'last', 'country_code'], how='left', indicator=True)
    excluded_df = df_all.loc[lambda x: x['_merge'] == 'left_only']
    print(excluded_df)
    print(len(excluded_df))

    # ---------------------- Outputs the CSV of athletes who changed countries ------------------------
    excluded_df.loc[:, ['first', 'last', 'nationality_x', 'country_code_x']].to_csv('changed_country')

    # -------------- Merge on first_name, last_name, country_code/nationality, and matched_events columns  -------------
    # matched_names_country_event = pd.merge(df1, df2, left_on=['first', 'last', 'nationality', 'sport'], right_on=['first', 'last', 'country_code', 'matched_events'], how='inner').loc[:, ['first', 'last', 'country_code', 'matched_events']]
    # outputs to csv
    # matched_names_country_event.to_csv('matched_names_country_event')

    # new testing testing
    # -------------- Merge on first_name, last_name, and matched_events columns  -------------
    # can take very long to run...
    # matched_names_country_event = pd.merge(df1, df2, left_on=['first', 'last', 'sport'], right_on=['first', 'last', 'matched_events'], how='inner').loc[:, ['first', 'last', 'country_code', 'matched_events']]
    # matched_names_country_event.to_csv('matched_names_event')

    # --------------------- Nicely prints out the desired dataframes ------------------------
    display_pretty_df(df1, df2, select_2020, excluded_df)


go(df_2016, df_2020)
