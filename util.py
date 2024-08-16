import sys
import json
import csv
import yaml

import copy

import pandas as pd
import numpy as np

import matplotlib as mpl

import time
from datetime import datetime
import pprint

import psycopg2
from sqlalchemy import create_engine, text as sql_text


def hello_world():
    print("Hello World!")

def build_query_listings_reviews(date1, date2):
    q31 = """Select distinct l.id, l.name 
    from listings l, reviews r
    where l.id = r.listing_id
    and r.date >= '"""
    q32 = """'
    and r.date >= '"""
    q33 = """'
    order by l.id; """
    return q31 + date1 + q32 + date2 + q33


# Calculate the difference between times 
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

# Calculate and return a dict with metrics for each year query 
def calc_time_diff(db_eng, count, query):
    time_list = []
    for i in range(count): 
        time_start = datetime.now()

        with db_eng.connect() as conn:
            df = pd.read_sql(query, con=conn)

        time_end = datetime.now()
        # Calulate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

    # Calulcate the metrics
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'exec_count': len(time_list),
        'timestamp': datetime.now().isoformat()
    }
    
    return perf_profile

def calc_time_diff3c(db_eng, count, query1, query2):
    time_list = []
    for i in range(count): 
        time_start = datetime.now()

        with db_eng.connect() as conn:
            df1 = pd.read_sql(query1, con=conn)
            df2 = pd.read_sql(query2, con=conn)

        time_end = datetime.now()
        # Calulate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

    # Calulcate the metrics
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'exec_count': len(time_list),
        'timestamp': datetime.now().isoformat()
    }
    return perf_profile


def time_diff_summary(db_eng, query, query_name, spec, all_indexes, count, json_file_name):
    perf_summary = fetch_perf_data(json_file_name)


    for index in all_indexes:
        if index not in spec:
            add_drop_index(db_eng, 'drop', index[0], index[1])
            
    
    for index in spec:
        add_drop_index(db_eng, 'add', index[0], index[1])


    perf_profile = calc_time_diff(db_eng, count, query)


    key_value = build_index_description_key(all_indexes, spec)


    if query_name in perf_summary:
        perf_dict = perf_summary[query_name]
    else:
        perf_dict = {}
    

    perf_dict[key_value] = perf_profile
    perf_summary[query_name] = perf_dict

    write_perf_data(perf_summary, json_file_name)
    return perf_summary

def time_diff_summary3c(db_eng, query1, query2, query_name, spec, all_indexes, count, json_file_name):

    perf_summary = fetch_perf_data(json_file_name)


    for index in all_indexes:
        if index not in spec:
            add_drop_index(db_eng, 'drop', index[0], index[1])
            

    for index in spec:
        add_drop_index(db_eng, 'add', index[0], index[1])


    perf_profile = calc_time_diff3c(db_eng, count, query1, query2)


    key_value = build_index_description_key3c(all_indexes, spec)


    if query_name in perf_summary:
        perf_dict = perf_summary[query_name]
    else:
        perf_dict = {}
    

    perf_dict[key_value] = perf_profile
    perf_summary[query_name] = perf_dict

    write_perf_data(perf_summary, json_file_name)
    return perf_summary

def time_diff_summary3b(db_eng, query, query_name, spec, all_indexes, count, json_file_name):
    perf_summary = fetch_perf_data(json_file_name)

    # drop unused indexes
#     for index in all_indexes:
#         if index not in spec:
#             add_drop_index(db_eng, 'drop', index[0], index[1])
            
    # add indexes to the corresponding tables
#     for index in spec:
#         add_drop_index(db_eng, 'add', index[0], index[1])

    perf_profile = calc_time_diff(db_eng, count, query)


    key_value = build_index_description_key(all_indexes, spec)


    if query_name in perf_summary:
        perf_dict = perf_summary[query_name]
    else:
        perf_dict = {}
    
    perf_dict[key_value] = perf_profile
    perf_summary[query_name] = perf_dict

    write_perf_data(perf_summary, json_file_name)
    return perf_summary

def build_query_listings_join_reviews(date1, date2):
    q31 = """Select distinct l.id, l.name 
    from listings l, reviews r
    where l.id = r.listing_id
    and r.date >= '"""
    q32 = """'
    and r.date <= '"""
    q33 = """'
    order by l.id; """
    return q31 + date1 + q32 + date2 + q33
    

def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)


def write_perf_data(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict2, fp)
        
def add_drop_index(db_eng, add_or_drop, col, table):
    q_create_index = f'''
    BEGIN TRANSACTION;
    CREATE INDEX IF NOT EXISTS {col}_in_{table}
    ON {table}({col});
    END TRANSACTION;
    '''

    q_drop_index = f'''
    BEGIN TRANSACTION;
    DROP INDEX IF EXISTS {col}_in_{table};
    END TRANSACTION;
    '''

    q_show_indexes = f'''
    select *
    from pg_indexes
    where tablename = '{table}';
    '''

    with db_eng.connect() as conn:
        if add_or_drop == 'add':
            conn.execute(sql_text(q_create_index))
        elif add_or_drop == 'drop':
            conn.execute(sql_text(q_drop_index))
        result = conn.execute(sql_text(q_show_indexes))
        return result.all()
    
def build_index_description_key(all_indexes, spec):
    key_value = "__"
    for index in all_indexes:
        if index in spec:
            key_value = key_value + f"{index[0]}_in_{index[1]}__"
    return key_value

def build_index_description_key3c(all_indexes, spec):
    key_value = "__"
    for index in all_indexes:
        if index in spec:
            if index[1] == 'listings':
                key_value = key_value + "neigh_in_listings__"
            else:
                key_value = key_value + f"{index[0]}_in_{index[1]}__"
    return key_value

def build_text_finder_index(date1, date2, word):
    q31 = """Select count(*)
    from reviews
    where comments_tsv @@ to_tsquery('"""
    q32 = """')
    and datetime >= '"""
    q33 = """'
    and datetime <= '"""
    return q31 + word + q32 + date1 + q33 + date2 + "';"

def build_text_finder_noindex(date1, date2, word):
    q31 = """Select count(*)
    from reviews
    where comments ILIKE '%% """
    q32 = """%%'
    and datetime >= '"""
    q33 = """'
    and datetime <= '"""
    return q31 + word + q32 + date1 + q33 + date2 + "';"

def update_neigh_add(neigh):
    q1 = """update reviews r
    set datetime = datetime + interval '5 days'
    from listings l
    where l.id = r.listing_id
    AND l.neighbourhood = '"""
    q2 = """'
    OR l.neighbourhood_group = '"""
    q3 = """'
    returning 'done'"""
    return q1 + neigh + q2 + neigh + q3 + ";"

def update_neigh_sub(neigh):
    q1 = """update reviews r
    set datetime = datetime - interval '5 days'
    from listings l
    where l.id = r.listing_id
    AND l.neighbourhood = '"""
    q2 = """'
    OR l.neighbourhood_group = '"""
    q3 = """'
    returning 'done'"""
    return q1 + neigh + q2 + neigh + q3 + ";"


def rename_keys(data):
    new_data = {}
    for key, value in data.items():
        if key.startswith("listings_join_review_"):
            year = key.split("_")[-1]
            new_data[year] = value
        else:
            new_data[key] = value
    return new_data

def extract_avg_values(data):
    avg_values = {}
    for year, details in data.items():
        avg_values[year] = {}
        for key, metrics in details.items():
            if 'avg' in metrics:
                avg_values[year][key] = metrics['avg']
    return avg_values

def extract_std_values(data):
    std_values = {}
    for year, details in data.items():
        std_values[year] = {}
        for key, metrics in details.items():
            if 'std' in metrics:
                std_values[year][key] = metrics['std']
    return std_values
    
    
def rename_keys_by_word(data, word):
    new_data = {}
    for key, value in data.items():
        if key.startswith(word+"_"):
            year = key.split("_")[-1]
            new_data[year] = value
    return new_data 
    
def rename_keys_3c(data):
    new_data = {}
    for key, value in data.items():
        if key.startswith("update_datetimes_query_"):
            nieghborhood = key.split("_")[-1]
            new_data[nieghborhood] = value
        else:
            new_data[key] = value
    return new_data 
    