from aksara.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge

class Table(GeneralChartsUtil) : 
    '''Table Class for table variables'''
    chart_type = 'TABLE'

    # Table Variables
    exclude = []
    freeze = []

    '''
    Initiailize the neccessary data for a table chart
    '''
    def __init__(self, full_meta, file_data, meta_data ,variable_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, meta_data ,variable_data, all_variable_data, file_src)

        self.chart_name = {'en' : self.variable_data['title_en'] , 'bm' : self.variable_data['title_bm']}
        self.exclude = self.meta_data['chart']['chart_filters']['EXCLUDE']
        self.freeze = self.meta_data['chart']['chart_filters']['FREEZE']
        
        self.metadata = self.rebuild_metadata()
        self.api = self.build_api()

        self.chart_details['chart'] = self.build_chart()
        self.db_input['catalog_data'] = self.build_catalog_data_info()

    '''
    Build the Table chart
    '''
    def build_chart(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        EXCLUDE = self.exclude

        res = {}

        if EXCLUDE :
            df = df.drop(EXCLUDE, axis=1)

        res['data'] = df.to_dict('records')
        res['columns'] = {'en' :{}, 'bm' : {}}

        for obj in self.all_variable_data[1:] : 
            if obj['name'] not in self.exclude : 
                res['columns']['en'][ obj['name'] ] = obj['title_en']
                res['columns']['bm'][ obj['name'] ] = obj['title_bm']

        return res

    def rebuild_metadata(self) : 
        self.metadata.pop('in_dataset', None)

        for i in self.all_variable_data[1:] : 
            i.pop('unique_id', None)

        out_data = []

        for obj in self.all_variable_data[1:] : 
            if obj['name'] not in self.exclude : 
                out_data.append(obj)

        self.metadata['out_dataset'] = out_data
        return self.metadata

    def build_api(self) : 
        res = {}
        res['freeze'] = self.freeze
        res['chart_type'] = self.chart_type

        return res