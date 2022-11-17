import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

def get_dict(d, keys):
    for key in keys:
        d = d[key]
    return d

def set_dict(d, keys, value):
    d = get_dict(d, keys[:-1])
    d[keys[-1]] = value

def slice_timeline(df, range_freq, keys_list, value_obj, cur_group, chart_name, operation) :
    timeline = {'DAILY' : 2, 'WEEKLY' : 5}
    group_time = {'WEEKLY' : 'W', 'MONTHLY' : 'M', 'YEARLY' : 'Y'}
    res = {}
    res['TABLE'] = {}
    res['TABLE']['columns'] = { 'x_en' : 'Date', 'y_en': chart_name['en'], 'x_bm' : 'Tarikh', 'y_bm' : chart_name['bm']}
    res['TABLE']['data'] = {}

    for range in range_freq : 
        res[range] = {}
        range_df = df.copy()
        if range in timeline : 
            last_date = pd.Timestamp(pd.to_datetime(range_df['date'].max()))
            start_date = pd.Timestamp(pd.to_datetime(last_date) - relativedelta(years=timeline[range]))
            range_df = range_df[(range_df.date >= start_date) & (range_df.date <= last_date)]

        if range in group_time : 
            key_list_mod = keys_list[:]
            key_list_mod.append('interval')
            range_df["interval"] = range_df["date"].dt.to_period( group_time[range] ).dt.to_timestamp()
            range_df['interval'] = range_df['interval'].values.astype(np.int64) // 10 ** 6
            new_temp_df = range_df.copy

            if operation == 'SUM' : 
                new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ value_obj['y'] ].sum()
            elif operation == 'MEAN' : 
                new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ value_obj['y'] ].mean()
            elif operation == 'MEDIAN' : 
                new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ value_obj['y'] ].median()

            res[range]['x'] = new_temp_df.groupby(keys_list)['interval'].get_group( cur_group ).to_list()
            res[range]['y'] = new_temp_df.groupby(keys_list)[ value_obj['y'] ].get_group( cur_group ).to_list()
            
            res['TABLE']['data'][range] = build_variable_table(res[range]['x'], res[range]['y'])
            
            if 'line' in value_obj : 
                res[range]['line'] = new_temp_df.groupby(keys_list)[ value_obj['y'] ].get_group( cur_group ).to_list()
        else : 
            range_df['date'] = range_df['date'].values.astype(np.int64) // 10 ** 6
            res[range]['x'] = range_df.groupby(keys_list)['date'].get_group(cur_group).to_list()
            res[range]['y'] = range_df.groupby(keys_list)[ value_obj['y'] ].get_group(cur_group).to_list()
            dma_col = value_obj['y'] if value_obj['line'] == '' else value_obj['line']
            res['TABLE']['data'][range] = build_variable_table(res[range]['x'], res[range]['y'])
            res[range]['line'] = range_df.groupby(keys_list)[dma_col].get_group( cur_group ).rolling(window=7).mean().replace({np.nan: None}).to_list()             

    return res

def get_range_values(freq) : 
    pos = ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY']
    index = pos.index(freq)
    return pos[index:]

def build_variable_table(x_arr, y_arr) :
    res = []
    
    for i in range(0, len(x_arr)) :
        data = {'x' : x_arr[i], 'y' : y_arr[i]}
        res.append(data)

    return res
