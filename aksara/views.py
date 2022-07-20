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

class Helper(APIView) : 
    def get(self, request, format=None):
        # Map and get all the parameters that exist in the URL
        param_list = dict(request.GET) 
        # Check if the parameters required for this API exists
        params_req = ["dashboard","helper-type"]
        if all (p in param_list for p in params_req) :
            dashboard_name = param_list['dashboard'][0]
            helper_type = param_list['helper-type'][0]
            location = param_list['location'][0]
            data = ''
            r_data = []
            if helper_type == "dropdown" : 
                filter = param_list['filter'][0]
                with_state = param_list['with_state'][0] if 'with_state' in param_list else False
                data = json.loads(areas.DROPDOWN_JSON)
                
                if with_state : # For Jitter
                    area_type = json.loads(areas.AREAS_JSON)
                    for x in data :
                        if filter in data[x]:
                            if with_state :
                                for d in data[x][filter]:
                                    d.update((k, v + ", " + x.upper()) for k, v in d.items() if k == "label")                    
                                r_data += sorted(data[x][filter], key=lambda d: d['label']) if with_state else data[x][filter]                    
                else : 
                    r_data = data[location][filter]
            elif helper_type == "area-type" :
                data = json.loads(areas.AREAS_JSON)
                r_data = {"area-type" : data[location], "area-name" : original_name(location)}
            if data != '' and location in data : 
                return JsonResponse(r_data, safe=False)
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

def original_name(area) :
    STATE_ABBR = {'jhr' : 'Johor',
                    'kdh' : 'Kedah',
                    'ktn' : 'Kelantan', 
                    'kvy' : 'Klang Valley',
                    'mlk' : 'Melaka',
                    'nsn' : 'Negeri Sembilan',
                    'phg' : 'Pahang',
                    'prk' : 'Perak',
                    'pls' : 'Perlis',
                    'png' : 'Pulau Pinang',
                    'sbh' : 'Sabah',
                    'swk' : 'Sarawak',
                    'sgr' : 'Selangor',
                    'trg' : 'Terengganu',
                    'lbn' : 'W.P. Labuan',
                    'pjy' : 'W.P. Putrajaya',
                    'kul' : 'W.P. Kuala Lumpur',
                    'mys' :'Malaysia'}

    if area in STATE_ABBR :
        return STATE_ABBR[area]
    else :
        HARD_CODED_AREAS = {
            'p.018-kulim-bandar-baharu' : 'P.018 Kulim-Bandar Baharu', 
            'n.27-layang-layang': 'N.27 Layang-Layang', 
            'n.20-api-api' : 'N.20 Api-Api', 
            'n.50-gum-gum' : 'N.50 Gum-Gum'}
        if area in HARD_CODED_AREAS :
            return HARD_CODED_AREAS[area]
        else :
            return area.replace("-", " ").title()