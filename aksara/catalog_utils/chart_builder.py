import pandas as pd
import numpy as np
from aksara.catalog_utils import chart_utils as cu
from mergedeep import merge

def timeseries_chart(file_name, variables, freq, chart_name) :
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})

    keys_list = variables['parents']
    value_obj = variables['format']

    for key in keys_list : 
        df[key] = df[key].apply(lambda x : x.lower().replace(' ', '-'))

    df['date'] = pd.to_datetime(df['date'])

    df['u_groups'] = list(df[keys_list].itertuples(index=False, name=None))
    u_groups_list = df['u_groups'].unique().tolist()

    res = {}
    range_values = cu.get_range_values(freq)

    for group in u_groups_list : 
        result = {}
        for b in group[::-1]:
            result = {b: result}

        cur_group = group[0] if len(group) == 1 else group
        
        cur_data = cu.slice_timeline(df, range_values, keys_list, value_obj, cur_group, chart_name)
        cu.set_dict(result, list(group), cur_data)
        
        merge(res, result)

    return res