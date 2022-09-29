import pandas as pd
import numpy as np
from pathlib import Path
from deepdiff import DeepDiff
import json, random
import math
import re
import copy

# Bar chart, pie chart, and doughnut chart builder
def bar_pie_chart(method_dict, variable_list, key) : 
    df = method_dict['function']['method'](**method_dict['parameters'])

    l1 = df[key].unique().tolist()
    d_dict = { x : {} for x in l1 }

    for chart_key, columns in variable_list.items() : 
        #If values are empty, exclude
        temp_df = df[df[columns].notnull().all(1)]
        c_val = temp_df.groupby(key).apply(lambda x: x[columns].to_dict(orient='records')).to_dict()
        key_list = temp_df[key].unique().tolist()
        for main_key in key_list : 
            d_dict[main_key][chart_key] = {}
            d_dict[main_key][chart_key]['id'] = chart_key
            d_dict[main_key][chart_key]['keys'] = columns
            d_dict[main_key][chart_key]['data'] = [{"id" : k, "value" :  v} for k, v in c_val[main_key][0].items()]

    return d_dict

# Pyramid chart builder
def pyramid_chart(chart_name, method_dict, variable_list, standard_key_name, key) : 
    df = method_dict['function']['method'](**method_dict['parameters'])
    l1 = df[key].unique().tolist()

    # Create base structure of dict here
    p_info = {}
    p_info[chart_name] = {}
    p_info[chart_name]['id'] = chart_name
    p_info[chart_name]['keys'] = standard_key_name
    p_info[chart_name]['data'] = []

    d_dict = { x : copy.deepcopy(p_info) for x in l1 }

    # Append data structure of dict here
    for k, v in variable_list.items():
        r_named = v[:len(standard_key_name)]
        mapping = dict(zip(r_named, standard_key_name))
        up_val = standard_key_name + v[len(standard_key_name):]
        c_val = df.rename(columns=mapping).groupby(key).apply(lambda x: x[up_val].to_dict(orient='records')).to_dict()
        for d in d_dict : 
            c_val[d][0]['id'] = k
            d_dict[d][chart_name]['data'].append(c_val[d][0])
    
    return d_dict

# Scatter chart, Jitter chart, line chart builder
def scatter_jitter_line_chart(chart_name, key, inner_key, variable_list, method_dict) : 
    df = method_dict['function']['method'](**method_dict['parameters'])    
    l1 = df[key].unique().tolist()

    # Create base structure of dict here
    p_info = {}
    p_info[chart_name] = {}
    p_info[chart_name]['id'] = chart_name
    p_info[chart_name]['data'] = []

    d_dict = { x : copy.deepcopy(p_info) for x in l1 }

    for x in variable_list.keys() : 
        d_dict[x][chart_name]['values'] = variable_list[x]

    for k, v in variable_list.items(): 
        for metric in v : 
            c_vals = ["x", "y"]
            temp = df.groupby(key).get_group(k)    
            mapping = {metric + "_x" : "x", metric + "_y" : "y"}
            per_metric = temp.rename(columns=mapping).groupby(inner_key).apply(lambda x: x[c_vals].to_dict(orient='records')).to_dict()
            
            arr = []
            for i_key, i_val in per_metric.items():
                i_dict = {}
                i_dict['id'] = i_key
                i_dict['data'] = i_val
                arr.append(i_dict)

            n_dict = {}
            n_dict[metric] = arr
            d_dict[k][chart_name]['data'].append(n_dict)

    return d_dict

# Waffle chart builder
def waffle_chart(chart_name, variable_list, method_dict) :
    main_key = variable_list['main_key'] # The first set of keys
    second_key = variable_list['second_key'] # Nested within the first set of keys
    data_key = variable_list['data_key'] # Key list of main information
    data_info = variable_list['data_info'] # Column of variable that will be included
    data_value = variable_list['data_value'] # Column of variables mapped with this column's value
    waffle_key = variable_list['waffle_key'] # The main key from data_info that will be in the waffle

    df = method_dict['function']['method'](**method_dict['parameters']) 
    key_list = df[main_key].unique().tolist()
    main_group = df.groupby(main_key)
    inner_keys = df[second_key].unique().tolist()
    data_key_list = df[data_key].unique().tolist()

    d_dict = {}

    for k in key_list : # Loop through each state
        d_dict[k] = {}
        d_dict[k][chart_name] = {}
        init_grouping = main_group.get_group(k) 
        for i in inner_keys : # Loop through each age group
            d_dict[k][chart_name][i] = {}
            category = init_grouping.groupby(second_key).get_group(i)
            for d in data_key_list : 
                tmp_dict = category.groupby(data_key).get_group(d).set_index(data_info).to_dict()[data_value]
                i_data = {}
                i_data['id'] = i + "_" + d
                i_data['label'] = i + "_" + d
                i_data['value'] = tmp_dict[waffle_key]
                tmp_dict['data'] = [i_data]
                tmp_dict['id'] = i + "_" + d
                tmp_dict['keys'] = [i + "_" + d]
                tmp_dict.pop(waffle_key, None)
                d_dict[k][chart_name][i][d] = tmp_dict

    return d_dict

# Choropleth chart builder
def choropleth_chart(variable_list, method_dict, chart_name) :
    df = method_dict['function']['method'](**method_dict['parameters']) 

    d_dict = {}
    metrics = df.columns.to_list()
    data_id_key = variable_list['data_id_key']
    categorize_by = variable_list['categories']

    for i in variable_list['columns_remove'] : 
        metrics.remove(i)

    c_dict = {}
    for i in df[categorize_by].unique().tolist() :
        c_dict[i] = {chart_name : []}

    for m in metrics :
        d_dict[m] = copy.deepcopy(c_dict)

    print(d_dict)

    for k, v in d_dict.items() : 
        for cat in v : 
            v[cat][chart_name] = df.groupby(categorize_by).get_group(cat).apply(lambda x: {'id' : getattr(x, data_id_key), 'value': getattr(x, k)}, axis=1).tolist()

    return d_dict  

# Geojson builders
def geo_chart(variable_list, method_dict) : 
    d_dict = {}

    for k,v in variable_list.items() : 
        f = open(v)
        data = json.load(f)
        geo_main = {}

        for x in range(0, len(data['features'])):
            area = data['features'][x]['properties'][k]
            cords = data['features'][x]['geometry']['coordinates']
            geo_info = {}

            area = method_dict['function']['method'](data['features'][x], k) 
            
            geo_info['name'] = area
            geo_info['area_type'] = k
            geo_info['shape_type'] = data['features'][x]['geometry']['type']
            geo_info['coordinates'] = cords

            geo_main[area] = {}
            geo_main[area]['geo_outline'] = {}
            geo_main[area]['geo_outline']['id'] = 'geo_outline'
            geo_main[area]['geo_outline']['keys'] = ['name', 'area_type', 'shape_type', 'coordinates']
            geo_main[area]['geo_outline']['data'] = geo_info

        d_dict.update(geo_main)

    return d_dict