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
from aksara.serializers import MetaSerializer, KKMSerializer
from aksara.models import MetaJson, KKMNowJSON

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