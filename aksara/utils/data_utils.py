from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from aksara.models import MetaJson, KKMNowJSON
from aksara.utils import dashboard_builder
from aksara.utils import triggers
from aksara.utils import common

import os
from os import listdir
from os.path import isfile, join
import json

'''
Operations to rebuild all meta, from each dashboard

HOW IT WORKS : 
    - Get each file within the META_JSON directory
    - Fetch data within file
    - If META doesn't exist, insert, else, update

'''

def rebuild_dashboard_meta(operation) :
    if operation == 'REBUILD' : 
        MetaJson.objects.all().delete()

    META_DIR = os.path.join(os.getcwd(), 'aksara/management/commands/META_JSON/')
    meta_files = []
    meta_files = [f for f in listdir(META_DIR) if isfile(join(META_DIR, f))]
    failed_builds = []

    for meta in meta_files : 
        try : 
            f_meta = META_DIR + meta
            f = open(f_meta)
            data = json.load(f)
            dbd_name = meta.replace(".json", "")
            
            updated_values = {'dashboard_meta' : data}
            obj, created = MetaJson.objects.update_or_create(dashboard_name=dbd_name, defaults=updated_values)
            obj.save()
            
            cache.set('META_' + dbd_name, data)
        except Exception as e :
            failed_obj = {}
            failed_obj['DASHBOARD_NAME'] = dbd_name
            failed_obj['ERROR'] = e
            failed_builds.append(failed_obj)

    if len(failed_builds) > 0 :
        err_message = triggers.format_multi_line(failed_builds, '--- FAILED META ---') 
        print(err_message)
        # triggers.send_telegram(err_message)
    else :
        print("Meta Built successfully.")
        # triggers.send_telegram("META Built Successfully.")


'''
Operations to rebuild all charts, from each dashboard.

HOW IT WORKS : 
    - Check what the operation is, if REBUILD, clear all existing chart data
    - Retrieve all meta jsons from db
    - Build all charts, according to charts within meta json

'''

def rebuild_dashboard_charts(operation) :
    if operation == 'REBUILD' : 
        KKMNowJSON.objects.all().delete()
    
    meta_json_list = MetaJson.objects.values()
    failed_builds = []

    data_as_of_list = {}

    # try : 
    #     data_as_of_file = os.path.join(os.getcwd(), 'KKMNOW_SRC/kkmnow-data-main') + '/metadata_updated_date.json'
    #     f = open(data_as_of_file)
    #     data_as_of_list = json.load(f)
    # except Exception as e:
    #     triggers.send_telegram("----- DATA UPDATE FILES NOT PRESENT -----")

    for meta in meta_json_list : 
        dbd_meta = meta['dashboard_meta']
        dbd_name = meta['dashboard_name']
        chart_list = dbd_meta['charts']

        for k in chart_list.keys() :
            chart_name = k
            chart_type = chart_list[k]['chart_type']
            c_data = {}
            c_data['variables'] = chart_list[k]['variables']
            c_data['input'] = chart_list[k]['chart_source']
            api_type = chart_list[k]['api_type']
            try:
                res = {}
                res['data'] = dashboard_builder.build_chart(chart_list[k]['chart_type'], c_data)
                if len(res['data']) > 0 : # If the dict isnt empty
                
                    if 'data_as_of' in chart_list[k] : 
                        res['data_as_of']  = chart_list[k]['data_as_of']

                    # if len(data_as_of_list) > 0 : # If the data update file exists 
                    #     data_update_info = get_latest_data_update([dbd_name, chart_name], data_as_of_list)
                    #     if data_update_info : 
                    #         res['data_as_of'] = data_update_info

                    updated_values = {'chart_type' : chart_type, 'api_type' : api_type, 'chart_data' : res}
                    obj, created = KKMNowJSON.objects.update_or_create(dashboard_name=dbd_name, chart_name=k, defaults=updated_values)
                    obj.save()
                    cache.set(dbd_name + "_" + k, res)
            except Exception as e:
                failed_obj = {}
                failed_obj['CHART_NAME'] = chart_name
                failed_obj['DASHBOARD'] = dbd_name
                failed_obj['ERROR'] = str(e)
                failed_builds.append(failed_obj)

    if len(failed_builds) > 0 :
        err_message = triggers.format_multi_line(failed_builds, '--- FAILED CHARTS ---') 
        print(err_message)
        # triggers.send_telegram(err_message)
    else : 
        print("Chart data built successfully")
        # triggers.send_telegram("Chart Data Built Successfully.")

'''
Operations to fetch the latest data update
'''

def get_latest_data_update(arr, data) : 
    for a in arr:
        if a in data:
            data = data[a]
        else:
            data = None 
            break
    
    return data


def rebuild_selective_update(changed_files) :
    failed_notify = {}

    if len(changed_files) > 0 :
 
        dashboard_list = set()
        data_as_of_list = {}
        failed_builds = []

        try : 
            data_as_of_file = os.path.join(os.getcwd(), 'KKMNOW_SRC/kkmnow-data-main') + '/metadata_updated_date.json'
            f = open(data_as_of_file)
            data_as_of_list = json.load(f)
        except Exception as e:
            triggers.send_telegram("----- DATA UPDATE FILES NOT PRESENT -----")

        for f in changed_files :
            meta_file = f.split('_')[0]
            if meta_file in common.FILE_NAME_CONVENTIONS : 
                dashboard_list.update( common.FILE_NAME_CONVENTIONS[ meta_file ] ) 

        for meta in dashboard_list : 
            meta_info = MetaJson.objects.filter(dashboard_name=meta).values('dashboard_meta')[0]['dashboard_meta']
            
            for k, v in meta_info['charts'].items() : 
                if v['chart_source'] in changed_files :
                    c_data = {}
                    c_data['variables'] = v['variables']
                    c_data['input'] = v['chart_source']

                    try:
                        res = {}
                        res['data'] = dashboard_builder.build_chart(v['chart_type'], c_data)
                        if len(res['data']) > 0 :
                            if len(data_as_of_list) > 0 : 
                                data_update_info = get_latest_data_update([meta, v['name']], data_as_of_list)
                                if data_update_info : 
                                    res['data_as_of'] = data_update_info
                            updated_values = {'chart_type' : v['chart_type'], 'api_type' : v['api_type'], 'chart_data' : res}
                            obj, created = KKMNowJSON.objects.update_or_create(dashboard_name=meta, chart_name=k, defaults=updated_values)
                            obj.save()
                            cache.set(meta + "_" + k, res)
                    except Exception as e:
                        failed_notify[meta] = False
                        failed_obj = {}
                        failed_obj['CHART_NAME'] = k
                        failed_obj['DASHBOARD'] = meta
                        failed_obj['ERROR'] = str(e)
                        failed_builds.append(failed_obj)
        
        if len(failed_builds) > 0 :
            err_message = triggers.format_multi_line(failed_builds, '--- FAILED CHARTS ---') 
            triggers.send_telegram(err_message)
        else : 
            triggers.send_telegram("Chart Data Built Successfully.")

        validate_info = {}
        validate_info['dashboard_list'] = dashboard_list 
        validate_info['failed_dashboards'] = failed_notify

        return validate_info
