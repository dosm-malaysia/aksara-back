from django.http import JsonResponse
from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache

from aksara.temp import areas, choropleth, geo, jitter, snapshot

import json

class Dashboard(APIView) :
    def get(self, request, format=None):
        # Map and get all the parameters that exist in the URL
        param_list = dict(request.GET) 
        # Check if the parameters required for this API exists
        params_req = ["dashboard","location"]
        if all (p in param_list for p in params_req) :
            #TODO : Check if the dashboard name exists in the db list of dashboards
            dashboard_name = param_list['dashboard'][0]
            l_name = param_list['location'][0]
            
            r_val = {}
            json_charts = {'bar_chart' : snapshot.DOUGHNUT_JSON,
                           'pyramid_chart' : snapshot.PYRAMID_JSON,
                           'geo_json' : geo.ALL_GEO_JSON,
                           'jitter_chart' : jitter.JITTER_JSON}

            area_info = json.loads(areas.AREAS_JSON)

            if l_name in area_info : 
                area_type = area_info[l_name]
                for c_type, c_json in json_charts.items() : 
                    c_info = json.loads(c_json)
                    if c_type == 'jitter_chart' : 
                        area_type = 'state' if area_type == 'country' else area_type
                        r_val[c_type] = c_info[area_type]
                    elif l_name in c_info :
                        r_val[c_type] = c_info[l_name]

            return JsonResponse(r_val, safe=False)
        else :
            return JsonResponse([], safe=False)

class Temporary(APIView) :
    def get(self, request, format=None):
        # Map and get all the parameters that exist in the URL
        param_list = dict(request.GET) 
        # Check if the dashboard parameter exists
        if 'dashboard' in param_list :
            dashboard_name = param_list['dashboard'][0]   
                     
            #TODO : Check if the dashboard name exists in the db
            return JsonResponse({}, safe=False)
        else :
            return JsonResponse([], safe=False)            