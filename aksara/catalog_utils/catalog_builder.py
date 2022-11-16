from aksara.catalog_utils import catalog_helper as ch
from aksara.catalog_utils import general_helper as gh
from aksara.catalog_utils import chart_picker as cp
from aksara.models import CatalogJson

import os
import pandas as pd

def test_build() :
    FILE_META = os.path.join(os.getcwd(), 'aksara/management/commands/temp/covid_cases.json')
    catalog_meta = gh.read_json(FILE_META)

    file = catalog_meta['file']
    data_variables = catalog_meta['catalog_data']

    for data in data_variables : 
        cur_id = data['id']
        unique_id = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(cur_id)
        res = {}

        db_input = { 'id' : unique_id, 
                    'catalog_meta' : catalog_meta, 
                    'catalog_name' : file['variables'][cur_id - 1]['name'],
                    'catalog_category' : file['category'],
                    'time_range' : data['catalog_filters']['frequency'],
                    'geographic' : 'STATE', # HARDCODED
                    'dataset_range' : str(data['catalog_filters']['start']) + '_' + str(data['catalog_filters']['end']), 
                    'data_source' : data['catalog_filters']['data_source']
                }
        
        res['chart_details'] = {}
        res['chart_details']['intro'] = ch.format_intro(file['variables'][cur_id - 1])
        res['chart_details']['intro']['unique_id'] = unique_id

        res['chart_details']['chart'] = cp.build_chart(file, data)

        res['explanation'] = data['metadata_lang'] # Builds the explanations

        res['metadata'] = ch.build_metadata_key(file, data, cur_id)

        res['downloads'] = {}
        res['downloads']['csv'] = file['link_csv']
        res['downloads']['parquet'] = file['link_parquet']

        api_filter = data['chart']['chart_filters']['SLICE_BY'][0]    
        df = pd.read_parquet(file['link_parquet'])
        fe_vals = df[api_filter].unique().tolist()
        be_vals = df[api_filter].apply(lambda x : x.lower().replace(' ', '-')).unique().tolist()

        res['API'] = {}
        res['API']['filter_default'] = be_vals[0]
        res['API']['mapping'] = dict(zip(fe_vals, be_vals))
        res['API']['chart_type'] = data['chart']['chart_type']
        ch.additional_info(file, data, data['chart']['chart_type'], res)

        db_input['catalog_data'] = res
        obj, created = CatalogJson.objects.update_or_create(id=unique_id, defaults=db_input)