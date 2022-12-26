from aksara.catalog_utils import general_helper as gh
from aksara.catalog_utils.catalog_variable_classes import Timeseries as tm
from aksara.catalog_utils.catalog_variable_classes import Choropleth as ch
from aksara.catalog_utils.catalog_variable_classes import Table as tb

from aksara.utils import cron_utils, data_utils
from aksara.models import CatalogJson

import os
from os import listdir
from os.path import isfile, join
import pathlib

def catalog_update(operation, op_method) :
    try : 
        opr_data = data_utils.get_operation_files(operation)
        operation = opr_data['operation']
        meta_files = opr_data['files']

        META_DIR = os.path.join(os.getcwd(), 'AKSARA_SRC/aksara-data-main/catalog/')

        if operation == 'REBUILD' : 
            CatalogJson.objects.all().delete()

        if not meta_files : 
            meta_files = [f for f in listdir(META_DIR) if isfile(join(META_DIR, f))]
        else : 
            meta_files = [f + ".json" for f in meta_files ]
        
        for meta in meta_files :
            FILE_META = os.path.join(os.getcwd(), 'AKSARA_SRC/aksara-data-main/catalog/' + meta)
            
            if pathlib.Path(meta).suffix == '.json': 
                data = gh.read_json(FILE_META)
                file_data = data['file']
                all_variable_data = data['file']['variables']
                catalog_data = data['catalog_data']
                full_meta = data
                file_src = meta.replace('.json', '') 

                for cur_data in catalog_data :
                    chart_type = cur_data['chart']['chart_type']
                    obj = []
                    variable_data = all_variable_data[ cur_data['id'] - 1 ]

                    if chart_type == 'TIMESERIES' :
                        obj = tm.Timeseries(full_meta, file_data, cur_data, variable_data, all_variable_data, file_src)
                    elif chart_type == 'CHOROPLETH' : 
                        obj = ch.Choropleth(full_meta, file_data, cur_data, variable_data, all_variable_data, file_src)
                    elif chart_type == 'TABLE' : 
                        variable_data = all_variable_data[0]
                        obj = tb.Table(full_meta, file_data, cur_data, variable_data, all_variable_data, file_src)

                    db_input = obj.db_input
                    unique_id = obj.unique_id
                    
                    db_obj, created = CatalogJson.objects.update_or_create(id=unique_id, defaults=db_input)
                    print(obj.variable_name + " : COMPLETED")
    except Exception as e: 
        print(e)    

def catalog_operation(operation, op_method) :
    try : 
        dir_name = 'AKSARA_SRC'
        zip_name = 'repo.zip'
        git_url = 'https://github.com/dosm-malaysia/aksara-data/archive/main.zip'
        git_token = os.getenv('GITHUB_TOKEN', '-')

        cron_utils.create_directory(dir_name)
        res = cron_utils.fetch_from_git(zip_name, git_url, git_token)

        if 'resp_code' in res and res['resp_code'] == 200 : 
            cron_utils.write_as_binary(res['file_name'], res['data'])
            cron_utils.extract_zip(res['file_name'], dir_name)
            catalog_update(operation, op_method)
    except Exception as e: 
        print(e)    
