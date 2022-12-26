from aksara.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge

class Timeseries(GeneralChartsUtil) : 
    '''Timeseries Class for timeseries variables'''
    chart_type = 'TIMESERIES'

    # API related fields
    api_filter = ''

    # Timeseries related fields
    frequency = ''
    applicable_frequency = []

    # Constant mappings
    timeseries_values = { 'DAILY' : 'Daily', 'WEEKLY' : 'Weekly', 'MONTHLY' : 'Monthly', 'YEARLY' : 'Yearly' }
    timeline = {'DAILY' : 2, 'WEEKLY' : 5}
    group_time = {'WEEKLY' : 'W', 'MONTHLY' : 'M', 'YEARLY' : 'Y'}


    # Chart related
    chart_name = {}
    t_keys = ''
    t_operation = ''
    t_format = ''

    '''
    Initiailize the neccessary data for a timeseries chart
    '''
    def __init__(self, full_meta, file_data, meta_data ,variable_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, meta_data ,variable_data, all_variable_data, file_src)

        if meta_data['chart']['chart_filters']['SLICE_BY'] : 
            self.api_filter = meta_data['chart']['chart_filters']['SLICE_BY'][0]
        else :
            self.api_filter = []

        self.frequency = meta_data['catalog_filters']['frequency']
        self.applicable_frequency = self.get_range_values()
        self.api = self.build_api_info()

        self.t_keys = meta_data['chart']['chart_variables']['parents']
        self.t_format = meta_data['chart']['chart_variables']['format']
        self.t_operation = meta_data['chart']['chart_variables']['operation']
        self.chart_name = {'en' : self.variable_data['title_en'] , 'bm' : self.variable_data['title_bm']}

        self.chart_details['chart'] = self.build_chart()
        self.db_input['catalog_data'] = self.build_catalog_data_info()

    '''
    Build the timeseries chart
    '''
    def build_chart(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        
        for key in self.t_keys : 
            df[key] = df[key].apply(lambda x : x.lower().replace(' ', '-'))

        df['date'] = pd.to_datetime(df['date'])

        if self.t_keys : 
            df['u_groups'] = list(df[self.t_keys].itertuples(index=False, name=None))
            u_groups_list = df['u_groups'].unique().tolist()

            res = {}

            for group in u_groups_list : 
                result = {}
                for b in group[::-1]:
                    result = {b: result}
                cur_group = group[0] if len(group) == 1 else group
                cur_data = self.slice_timeline(df, cur_group)
                self.set_dict(result, list(group), cur_data)
                merge(res, result)

            return res
        else :
            res = self.slice_timeline(df, '')
            return res

    '''
    Slice the timeseries based on conditions of the frequency of when data is updated
    '''
    def slice_timeline(self, df, cur_group) : 
        res = {}
        res['TABLE'] = {}
        res['TABLE']['columns'] = { 'x_en' : 'Date', 'y_en': self.chart_name['en'], 'x_bm' : 'Tarikh', 'y_bm' : self.chart_name['bm']}
        res['TABLE']['data'] = {}

        for range in self.applicable_frequency : 
            res[range] = {}
            range_df = df.copy()
            if range in self.timeline : 
                last_date = pd.Timestamp(pd.to_datetime(range_df['date'].max()))
                start_date = pd.Timestamp(pd.to_datetime(last_date) - relativedelta(years=self.timeline[range]))
                range_df = range_df[(range_df.date >= start_date) & (range_df.date <= last_date)]

            if range in self.group_time : 
                key_list_mod = self.t_keys[:] if self.t_keys else []
                key_list_mod.append('interval')
                range_df["interval"] = range_df["date"].dt.to_period( self.group_time[range] ).dt.to_timestamp()
                range_df['interval'] = range_df['interval'].values.astype(np.int64) // 10 ** 6
                new_temp_df = range_df.copy

                if self.t_operation == 'SUM' : 
                    new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ self.t_format['y'] ].sum()
                elif self.t_operation == 'MEAN' : 
                    new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ self.t_format['y'] ].mean()
                elif self.t_operation == 'MEDIAN' : 
                    new_temp_df = range_df.groupby(key_list_mod, as_index=False)[ self.t_format['y'] ].median()

                if self.t_keys :
                    res[range]['x'] = new_temp_df.groupby(self.t_keys)['interval'].get_group( cur_group ).to_list()
                    res[range]['y'] = new_temp_df.groupby(self.t_keys)[ self.t_format['y'] ].get_group( cur_group ).to_list()
                else : 
                    res[range]['x'] = new_temp_df['interval'].to_list()
                    res[range]['y'] = new_temp_df[ self.t_format['y'] ].to_list()

                res['TABLE']['data'][range] = self.build_variable_table(res[range]['x'], res[range]['y'])
                
                if 'line' in self.t_format : 
                    if self.t_keys :
                        res[range]['line'] = new_temp_df.groupby(self.t_keys)[ self.t_format['y'] ].get_group( cur_group ).to_list()
                    else : 
                        res[range]['line'] = new_temp_df[ self.t_format['y'] ].to_list()
            else : 
                range_df['date'] = range_df['date'].values.astype(np.int64) // 10 ** 6
                
                if self.t_keys :                 
                    res[range]['x'] = range_df.groupby(self.t_keys)['date'].get_group( cur_group ).to_list()
                    res[range]['y'] = range_df.groupby(self.t_keys)[ self.t_format['y'] ].get_group( cur_group ).to_list()
                    dma_col = self.t_format['y'] if self.t_format['line'] == '' else self.t_format['line']
                    res['TABLE']['data'][range] = self.build_variable_table(res[range]['x'], res[range]['y'])
                    res[range]['line'] = range_df.groupby(self.t_keys)[dma_col].get_group( cur_group ).rolling(window=7).mean().replace({np.nan: None}).to_list()             
                else : 
                    res[range]['x'] = range_df['date'].to_list()
                    res[range]['y'] = range_df[ self.t_format['y'] ].to_list()
                    dma_col = self.t_format['y'] if self.t_format['line'] == '' else self.t_format['line']
                    res['TABLE']['data'][range] = self.build_variable_table(res[range]['x'], res[range]['y'])
                    res[range]['line'] = range_df[dma_col].rolling(window=7).mean().replace({np.nan: None}).to_list()
        return res

    '''
    Gets the possible range of values for the dataset
    '''
    def get_range_values(self) : 
        pos = ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY']
        index = pos.index(self.frequency)
        return pos[index:]

    '''
    Builds the API info for timeseries
    '''
    def build_api_info(self) : 
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if self.api_filter : 
            fe_vals = df[self.api_filter].unique().tolist()
            be_vals = df[self.api_filter].apply(lambda x : x.lower().replace(' ', '-')).unique().tolist()
            filter_obj = self.build_api_object_filter('filter', fe_vals[0], be_vals[0], dict(zip(fe_vals, be_vals)))
            api_filters_inc.append(filter_obj)
        
        range_options = [ {'label' : self.timeseries_values[r], 'value' : r} for r in self.applicable_frequency ]        
        range_obj = self.build_api_object_filter('range', self.timeseries_values[ self.frequency ], self.frequency, range_options)
        api_filters_inc.append(range_obj)

        res['API'] = {}
        res['API']['filters'] = api_filters_inc
        res['API']['chart_type'] = self.meta_data['chart']['chart_type']

        return res['API']