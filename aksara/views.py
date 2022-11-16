from django.http import JsonResponse
from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from aksara.utils import cron_utils, triggers
from aksara.serializers import MetaSerializer, KKMSerializer, CatalogSerializer
from aksara.models import MetaJson, KKMNowJSON, CatalogJson

from threading import Thread

import json
import os
import environ

env = environ.Env()
environ.Env.read_env()

class KKMNOW(APIView):
    def post(self, request, format=None):
        if is_valid_request(request, os.getenv("WORKFLOW_TOKEN")) :
            thread = Thread(target=cron_utils.selective_update)
            thread.start()
            return Response(status=status.HTTP_200_OK)
        
        return JsonResponse({'status':401,'message':"unauthorized"}, status=401)


    def get(self, request, format=None):
        # if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")) :
        #     return JsonResponse({'status': 401,'message':"unauthorized"}, status=401)

        param_list = dict(request.GET)
        params_req = ["dashboard"]

        if all(p in param_list for p in params_req):
            res = handle_request(param_list)
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)

class DATA_VARIABLE(APIView):
    def get(self, request, format=None):
        param_list = dict(request.GET)
        params_req = ["id"]

        if all(p in param_list for p in params_req):
            res = data_variable_handler(param_list)
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)

class DATA_CATALOG(APIView) :
    def get(self, request, format=None):
        param_list = dict(request.GET)
        info = CatalogJson.objects.all().values('id', 'catalog_name', 'catalog_category')
        res = {}

        for item in info.iterator():
            category = item['catalog_category'] 
            item.pop('catalog_category', None)
            if category not in res : 
                temp = [item]
                res[category] = temp
            else : 
                res[category].append(item)

        return JsonResponse(res, safe=False)

def data_variable_chart_handler(data, chart_type, param_list) : 
    if chart_type == 'TIMESERIES' :
        filter = data['API']['filter_default']
        range = data['API']['range_default']

        if 'filter' in param_list : 
            filter = param_list['filter'][0]

        if 'range' in param_list : 
            range =  param_list['range'][0]

        table_data = {}
        table_data['columns'] = data['chart_details']['chart'][filter]['TABLE']['columns'] 
        table_data['data'] = data['chart_details']['chart'][filter]['TABLE']['data'][range]
        chart_data = data['chart_details']['chart'][filter][range]
        intro = data['chart_details']['intro']

        return {'chart_data' : chart_data, 'table_data' : table_data, 'intro' : intro}


def data_variable_handler(param_list) :
    var_id = param_list["id"][0]
    info = CatalogJson.objects.filter(id=var_id).values('catalog_data')
    info = info[0]['catalog_data']
    chart_type = info['API']['chart_type']
    
    info['chart_details'] = data_variable_chart_handler(info, chart_type, param_list)

    if len(info) == 0 : 
        return {}
    return info

def handle_request(param_list):
    dbd_name = str(param_list["dashboard"][0])
    dbd_info = cache.get("META_" + dbd_name)

    if not dbd_info :
        dbd_info = MetaJson.objects.filter(dashboard_name=dbd_name).values('dashboard_meta')

    params_req = []

    if len(dbd_info) > 0:
        dbd_info = dbd_info if isinstance(dbd_info, dict) else dbd_info[0]["dashboard_meta"]
        params_req = dbd_info["required_params"]

    res = {}
    if all(p in param_list for p in params_req):
        data = dbd_info['charts']

        if len(data) > 0 :
            for k, v in data.items() :
                api_type = v['api_type']
                api_params = v['api_params']
                cur_chart_data = cache.get(dbd_name + "_" + k)

                if not cur_chart_data :
                    cur_chart_data = KKMNowJSON.objects.filter(dashboard_name=dbd_name, chart_name=k).values('chart_data')[0]['chart_data']                    
                    cache.set(dbd_name + "_" + k, cur_chart_data)

                data_as_of = None if 'data_as_of' not in cur_chart_data else cur_chart_data['data_as_of']

                if api_type == 'static' :
                    res[k] = {}
                    if data_as_of : 
                        res[k]['data_as_of'] = data_as_of 
                    res[k]['data'] = cur_chart_data['data']
                else : 
                    if len(api_params) > 0:
                        cur_chart_data = get_nested_data(api_params, param_list, cur_chart_data['data'])
                    
                    if len(cur_chart_data) > 0:
                        res[k] = {}
                        if data_as_of : 
                            res[k]['data_as_of'] = data_as_of                      
                        res[k]['data'] = cur_chart_data
    return res

'''
Slices dictionary,
based on keys within dictionary
'''

def get_nested_data(api_params, param_list, data) :
    for a in api_params:
        if a in param_list:
            key = param_list[a][0] if "__FIXED__" not in a else a.replace("__FIXED__", "")
            if key in data:
                data = data[key]
        else:
            data = {}
            break
    
    return data

'''
Checks whether or not,
a request made is valid
'''

def is_valid_request(request, workflow_token) : 
    if "Authorization" not in request.headers:
        return False
        
    secret = request.headers.get("Authorization")
    if secret != workflow_token:
        return False

    return True    