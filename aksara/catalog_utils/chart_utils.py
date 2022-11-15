import pandas as pd
from dateutil.relativedelta import relativedelta

def get_dict(d, keys):
    for key in keys:
        d = d[key]
    return d

def set_dict(d, keys, value):
    d = get_dict(d, keys[:-1])
    d[keys[-1]] = value

def slice_timeline(df, freq) :
    timeline = {'DAILY' : 2, 'WEEKLY' : 5}
    
    if freq in timeline : 
        last_date = pd.Timestamp(pd.to_datetime(df['date'].max()))
        start_date = pd.Timestamp(pd.to_datetime(last_date) - relativedelta(years=timeline[freq]))
        df = df[(df.date >= start_date) & (df.date <= last_date)]    
    
    return df