import pandas as pd
import numpy as np
from aksara.catalog_utils import chart_utils as cu
from mergedeep import merge

def timeseries_chart(file_name, variables, freq) :
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})

    keys_list = variables['parents']
    value_obj = variables['format']

    for key in keys_list : 
        df[key] = df[key].apply(lambda x : x.lower().replace(' ', '-'))

    df['date'] = pd.to_datetime(df['date'])
    df = cu.slice_timeline(df, freq)
    df['date'] = df['date'].values.astype(np.int64) // 10 ** 6
    
    df['u_groups'] = list(df[keys_list].itertuples(index=False, name=None))
    u_groups_list = df['u_groups'].unique().tolist()

    res = {}
    for group in u_groups_list : 
        result = {}
        for b in group[::-1]:
            result = {b: result}
        for k, v in value_obj.items() :
            group_l = group + (k, )
            temp_group = group[0] if len(group) == 1 else group
            if k == 'line' :
                dma_col = variables['format']['y'] if v == '' else v
                dma_vals = df.groupby(keys_list)[dma_col].get_group( temp_group ).rolling(window=7).mean().replace({np.nan: None}).to_list()
                cu.set_dict(result, list(group_l), dma_vals)
            else : 
                cu.set_dict(result, list(group_l), df.groupby(keys_list)[v].get_group( temp_group ).to_list())
        merge(res, result)

    return res