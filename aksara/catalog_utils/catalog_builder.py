from aksara.catalog_utils import catalog_helper as ch
from aksara.catalog_utils import general_helper as gh
from aksara.catalog_utils import chart_picker as cp
from aksara.utils import cron_utils
from aksara.models import CatalogJson

import os
from os import listdir
from os.path import isfile, join
import pathlib
import pandas as pd

def test_build() :
    dir_name = 'AKSARA_SRC'
    zip_name = 'repo.zip'
    git_url = 'https://github.com/dosm-malaysia/aksara-data/archive/main.zip'
    git_token = os.getenv('GITHUB_TOKEN', '-')

    cron_utils.create_directory(dir_name)
    res = cron_utils.fetch_from_git(zip_name, git_url, git_token)

    if 'resp_code' in res and res['resp_code'] == 200 : 
        cron_utils.write_as_binary(res['file_name'], res['data'])
        cron_utils.extract_zip(res['file_name'], dir_name)

        META_DIR = os.path.join(os.getcwd(), 'AKSARA_SRC/aksara-data-main/')
        meta_files = [f for f in listdir(META_DIR) if isfile(join(META_DIR, f))]

        for meta in meta_files :
            FILE_META = os.path.join(os.getcwd(), 'AKSARA_SRC/aksara-data-main/' + meta)
            if pathlib.Path(meta).suffix == '.json': 

                catalog_meta = gh.read_json(FILE_META)

                file = catalog_meta['file']
                data_variables = catalog_meta['catalog_data']

                for data in data_variables : 
                    cur_id = data['id']
                    unique_id = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(cur_id)
                    res = {}

                    db_input = { 'id' : unique_id, 
                                'catalog_meta' : catalog_meta, 
                                'catalog_name' : file['variables'][cur_id - 1]['title_en'] + ' | ' + file['variables'][cur_id - 1]['title_bm'],
                                'catalog_category' : file['category'],
                                'time_range' : data['catalog_filters']['frequency'],
                                'geographic' : ' | '.join(data['catalog_filters']['geographic']),
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
                    res['API']['filters'] = []
                    res['API']['chart_type'] = data['chart']['chart_type']
                    filter = {
                        'key' : 'filter',
                        'default' : {
                            'label' : fe_vals[0],
                            'value' : be_vals[0]
                        },
                        'options' : [ {'label' : k, 'value' : v} for k, v in dict(zip(fe_vals, be_vals)).items() ]
                    }
                    res['API']['filters'].append(filter)

                    # res['API']['filter_default'] = be_vals[0]
                    # res['API']['mapping'] = dict(zip(fe_vals, be_vals))
                    ch.additional_info(file, data, data['chart']['chart_type'], res['API']['filters'])

                    db_input['catalog_data'] = res
                    obj, created = CatalogJson.objects.update_or_create(id=unique_id, defaults=db_input)
    
