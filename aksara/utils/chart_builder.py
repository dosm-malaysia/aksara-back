import pandas as pd
import json
import numpy as np
import copy
import ast
from mergedeep import merge
import datetime
from aksara.utils.general_chart_helpers import *
from aksara.utils.operations import *
from dateutil.relativedelta import relativedelta

'''
Builds Bar Chart
'''
def bar_chart(file_name, variables) :
    df = pd.read_parquet(file_name)

    if 'state' in df.columns : 
        df['state'].replace(STATE_ABBR, inplace=True)
    df = df.replace({np.nan: None})

    keys = variables['keys']
    axis_values = variables['axis_values']

    df['u_groups'] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df['u_groups'].unique().tolist()

    res = {}

    for group in u_groups_list : 
        result = {}
        for b in group[::-1]:
            result = {b: result}
        if isinstance(axis_values, list) : 
            # group = group[0] if len(group) == 1 else group
            x_list = df.groupby(keys)[axis_values[0]].get_group(group).to_list()
            y_list = df.groupby(keys)[axis_values[1]].get_group(group).to_list()
            final_d = {'x' : x_list, 'y' : y_list}
            set_dict(result, list(group), final_d, 'SET')
        else :
            final_d = {}
            for k, v in axis_values.items() :
                if k == 'X_Y' :
                    final_d['x'] = v
                    final_d['y'] = df.groupby(keys).get_group(group)[v].values.flatten().tolist()
                else :    
                    final_d[k] = df.groupby(keys)[v].get_group(group).to_list()
            set_dict(result, list(group), final_d, 'SET')
        merge(res, result)

    return res

'''
Builds Bar Meter
'''
def bar_meter(file_name, variables) :
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: variables['null_vals']})

    if 'state' in df.columns : 
        df['state'].replace(STATE_ABBR, inplace=True)

    keys = variables['keys']
    axis_values = variables['axis_values']
    final_key_cols = []
    add_key = variables['add_key']
    wanted = variables['wanted']
    id_needed = variables['id_needed']
    condition = variables['condition']
    post_op = variables['post_operation']
    
    if len(wanted) > 0 : 
        for i in wanted : 
            df = df[df[ i['col_name'] ].isin( i['values'] )]

    for i in axis_values : 
        for k, v in i.items() :
            columns = [k, v] + list(add_key.keys())
            if id_needed :
                columns.append('id')
            df['id'] = df[ k ]
            rename_cols = {k : 'x', v : 'y'}
            rename_cols.update(add_key)
            df['final_' + v] = df[ columns ].rename(columns=rename_cols).apply(lambda s: s.to_dict(), axis=1)
            final_key_cols.append('final_' + v)

    res = {}
    
    if len( keys ) != 0 : 
        df['u_groups'] = list(df[keys].itertuples(index=False, name=None))
        u_groups_list = df['u_groups'].unique().tolist()

        for group in u_groups_list : 
            result = {}
            for b in group[::-1]:
                result = {b: result}
            for i in final_key_cols :
                group_l = [group[0]] if len(group) == 1 else list(group)
                group = group[0] if len(group) == 1 else group
                temp = df.groupby(keys)[i].get_group(group).to_list()
                temp = perform_operation(temp, post_op)
                set_dict(result, group_l, temp, 'SET')
            merge(res, result)
    else : 
        res = {}
        for i in final_key_cols :
            key = i.replace('final_', '')
            res[key] = df[i].to_list() 

    return res

'''
Builds Choropleth
'''
def choropleth_chart(file_name, variables) :
    df = pd.read_parquet(file_name)
    # if 'state' in df.columns :
    #     df['state'].replace(STATE_ABBR, inplace=True)
    # df.rename(columns={'state' : 'id'})
    df['id'] = df[ variables['id'] ]
    cols = variables['cols']
    cols.append('id')
    df['json'] = df[ cols ].to_dict(orient='records')

    res = df['json'].tolist()

    return res

'''
Builds Custom Chart
'''
def custom_chart(file_name, variables) :
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: variables['null_vals']})
    if 'state' in df.columns : 
        df['state'].replace(STATE_ABBR, inplace=True)

    keys = variables['keys']

    df['data'] = df[ variables['columns'] ].to_dict(orient="records")
    
    df['u_groups'] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df['u_groups'].unique().tolist()

    res = {}

    for group in u_groups_list : 
        result = {}
        for b in group[::-1]:
            result = {b: result}
        group_l = [group[0]] if len(group) == 1 else list(group)
        group = group[0] if len(group) == 1 else group
        temp = df.groupby(keys)['data'].get_group(group).to_list()[0]
        set_dict(result, group_l, temp, 'SET')
        merge(res, result)
    
    return res

'''
Builds Heatmap Chart
'''
def heatmap_chart(file_name, variables) :
    cols = variables['cols']
    id = variables['id']
    keys = variables['keys']
    replace_vals = variables['replace_vals']
    dict_rename = variables['dict_rename']
    operation = variables['operation']
    row_format = variables['row_format']

    df = pd.read_parquet(file_name)
    df['id'] = df[id] # Create ID first, cause ID may be 'state'
    if 'state' in df.columns : 
        df['state'].replace(STATE_ABBR, inplace=True)
    df = df.replace({np.nan: variables['null_values']})
    col_list = []

    if isinstance(cols, list) : 
        for x in cols :
            temp_str = 'x_' + x
            df[ temp_str ] = x.title() if row_format == 'title' else x.upper()
            df[x] = df[x].replace(replace_vals, regex=True)
            df['json_' + x] = df[ [temp_str, x] ].rename(columns={x : 'y', temp_str : 'x'}).apply(lambda s: s.to_dict(), axis=1)
            col_list.append('json_' + x)
    else :
        rename_cols = { cols['x'] : 'x', cols['y'] : 'y' }
        df['json'] = df[ [cols['x'], cols['y']] ].rename(columns=rename_cols).apply(lambda s: s.to_dict(), axis=1)
        col_list.append('json')


    if df['id'].dtype != 'int32' :
        df['id'] = df['id'].apply(lambda x : rename_labels(x, dict_rename))

    df['data'] = df[ col_list ].values.tolist()
    df['final'] = df[ ['id', 'data'] ].apply(lambda s: s.to_dict(), axis=1)

    res = prepopulate_dict(keys, df)

    for index, row in df.iterrows():  
        k_list = []
        for k in keys :
            k_list.append(row[k])
        set_dict(res, k_list, row['final'], operation)

    return res

'''
Builds Snapshot Chart
'''
def snapshot_chart(file_name, variables) :
    df = pd.read_parquet(file_name)
    if 'state' in df.columns : 
        df['state'].replace(STATE_ABBR, inplace=True)
    df = df.replace({np.nan: variables['null_vals']})

    main_key = variables['main_key']
    replace_word = variables['replace_word']
    data = variables['data']

    record_list = list(data.keys())
    record_list.append('index')
    record_list.append(main_key)

    # df['index'] = range(0, len(df[main_key].unique()))
    df['index'] = range(0, len(df[main_key]))

    changed_cols = {}
    for k, v in data.items() :
        if replace_word != '' :
            changed_cols = {x : x.replace(k, replace_word) for x in v }
        df[k] = df[v].rename(columns = changed_cols).apply(lambda s: s.to_dict(), axis=1)

    res_dict = df[record_list].to_dict(orient="records")
    
    res_json = {}
    v2_res = []

    for i in res_dict :
        # v1 code
        # res_json[ i[main_key] ] = i

        # v2 Code
        v2_res.append(i)

    # Pushes all the items into an array
    # v1 Code
    # res = []
    # for k, v in res_json.items() :
    #     res.append( res_json[k] )

    res = v2_res

    return res

'''
Builds Timeseries Chart
'''
def timeseries_chart(file_name, variables) :
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: 0})
    
    DATE_RANGE = ''
    if 'DATE_RANGE' in variables : 
        DATE_RANGE = variables['DATE_RANGE']
        variables.pop('DATE_RANGE') # Pop after use to not effect the rest of the code
    
    df['date'] = pd.to_datetime(df['date'])

    if '_YEARS' in DATE_RANGE :
        DATE_RANGE = int( DATE_RANGE.replace('_YEARS', '') )
        last_date = pd.Timestamp(pd.to_datetime(df['date'].max()))
        start_date = pd.Timestamp(pd.to_datetime(last_date) - relativedelta(years=DATE_RANGE))
        df = df[(df.date >= start_date) & (df.date <= last_date)]


    df['date'] = df['date'].values.astype(np.int64) // 10 ** 6

    if 'state' in df.columns:
        df['state'].replace(STATE_ABBR, inplace=True)

    keys_list = []
    get_nested_keys(variables, keys_list, 'KEYS')
    keys_list = keys_list[::-1]

    value_obj = []
    get_nested_keys(variables, value_obj, 'VALUES')

    if len(keys_list) == 0 : 
        res = {}
        for k, v in variables.items() :
            res[k] = df[v].to_list()
    else :
        df['u_groups'] = list(df[keys_list].itertuples(index=False, name=None))
        u_groups_list = df['u_groups'].unique().tolist()

        res = {}
        for group in u_groups_list : 
            result = {}
            for b in group[::-1]:
                result = {b: result}
            for k, v in value_obj[0].items() :
                group_l = group + (k, )
                temp_group = group[0] if len(group) == 1 else group
                set_dict(result, list(group_l), df.groupby(keys_list)[v].get_group( temp_group ).to_list(), 'SET')
            merge(res, result)

    return res

'''
Builds Waffle Chart
'''
def waffle_chart(file_name, variables) :
    df = pd.read_parquet(file_name)
    df['state'].replace(STATE_ABBR, inplace=True)

    wanted = variables['wanted']
    group = variables['groups']
    dict_keys = variables['dict_keys']
    data_arr = variables['data_arr']
    
    key_value = []

    if len(wanted) > 0 : 
        df = df[df['age_group'].isin(wanted)]
    
    for k, v in data_arr.items() : 
        if isinstance(v, dict) :
            key_value.append(list(v.keys())[0])
            key_value.append(list(v.values())[0])
        else :
            df[k] = df[v]


    df['data'] = df[ list(data_arr.keys()) ].apply(lambda s: s.to_dict(), axis=1) 

    df['u_groups'] = list(df[ group ].itertuples(index=False, name=None))
    u_groups_list = df['u_groups'].unique().tolist()

    res = {}
    for groups in u_groups_list : 
        result = {}
        for b in groups[::-1]:
            result = {b: result}
        cur_df = df.groupby(group)[ dict_keys ].get_group(groups)
        temp_df = df.groupby(group)[[key_value[0], 'data']].get_group(groups)

        temp_dict = dict(cur_df.values)
        temp_dict['data'] = temp_df['data'].loc[ temp_df[ key_value[0] ] == key_value[1]].to_list()            
        set_dict(result, list(groups), temp_dict, 'SET')
        merge(res, result)

    return res

'''
Custom helpers for facilities
'''
def helpers_custom(file_name) :
    df = pd.read_parquet(file_name)
    df['state'].replace(STATE_ABBR, inplace=True)

    state_mapping = {}
    state_mapping['facility_types'] = df['type'].unique().tolist()
    state_mapping['state_district_mapping'] = {}

    for state in df['state'].unique() :
        state_mapping['state_district_mapping'][state] = df.groupby('state').get_group(state)['district'].unique().tolist()

    return state_mapping