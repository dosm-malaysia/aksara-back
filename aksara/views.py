from django.http import JsonResponse
from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache

from .serializers import MetaSerializer, KKMSerializer
from .models import MetaJson, KKMNowJSON

from aksara.temp import areas, choropleth, geo, jitter, snapshot
from aksara.json_helpers import formatter

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
                    temp = {location : data[location]}
                    temp.update(data)
                    for x in temp :
                        if filter in temp[x]:
                            if with_state :
                                for d in temp[x][filter]:
                                    d.update((k, v + ", " + x.upper()) for k, v in d.items() if k == "label")                    
                                r_data += sorted(temp[x][filter], key=lambda d: d['label']) if with_state else temp[x][filter]                    
                else : 
                    r_data = data[location][filter]
            elif helper_type == "area-type" :
                data = json.loads(areas.AREAS_JSON)
                r_data = {"area-type" : data[location], "area-name" : original_name(location)}
            if data != '' and location in data : 
                return JsonResponse(r_data, safe=False)
        else : 
            return JsonResponse([], safe=False)

class Test(APIView) :
    def get(self, request, format=None):
        param_list = dict(request.GET) # Map and get all the parameters that exist in the URL

        # Check for params: required parameters for dashboard is dashboard, optional is page
        ''' 
            Error Handling : 1. Check if dashboard param exists
                             2. Check if page param exists
                             3. Check if META json of dashboard exists
                             4. Check if page exists in META json
        '''
        
        if 'dashboard' in param_list :
            dashboard_name = param_list['dashboard'][0]   
            page = default_params('page', param_list, 'main')
            r_val = {} # Set default return value

            # Fetch META json here
            dbd_info = MetaJson.objects.get(dashboard_name = dashboard_name + "_meta")
            dbd_json = MetaSerializer(dbd_info, many=False).data['dashboard_meta']

            page_json = dbd_json['pages'][page] # Retrieve page information here
            page_params = page_json['params']['required'] # Retrieve required page parameters here
            
            if required_params(page_params, param_list) : # Check if all required dashboard params exists
                dbd_charts = page_json['charts']
                for i in dbd_charts :
                    required_chart_params = dbd_charts[i]['parameters']['dashboard']['required']['params']
                    if set(required_chart_params).issubset(page_params) : # Check if the required params for the charts exist on the current API call
                        chart_type = dbd_charts[i]['chart_type']
                        dummy_data = {"mys": {"info": {"test": ["mys", "sgr", "ktn"]}}, "sgr": ["mys", "sgr", "ktn"], "mlk" : "sgr"}
                        
                        c_val =  slice_json_by_params(required_chart_params, param_list, dummy_data)

                        if dbd_charts[i]['json_method']['post'] is not None : 
                            post = getattr(formatter, dbd_charts[i]['json_method']['post']['method'])
                            method_args = dbd_charts[i]['json_method']['post']['argument']
                            method_args['data'] = c_val
                            c_val = post(method_args)

                        if chart_type not in r_val : 
                            r_val[chart_type] = {}

                        r_val[chart_type][i] = c_val

            return JsonResponse(r_val, safe=False)
        else :
            return JsonResponse({}, safe=False)

class KKMNOW(APIView) :
    def get(self, request, format=None):
        param_list = dict(request.GET)
        params_req = ['dashboard']
        
        if all (p in param_list for p in params_req) :
            res = {}

            '''
            If/else below is temporary, usinf now only to work development faster.
            Remove if/elif and create general function once all API's are up.
            '''

            if param_list['dashboard'][0] == 'blood_donation' :
                res = blood_donation(param_list)
            elif param_list['dashboard'][0] == 'covidvax' :
                res = covidvax(param_list)
            elif param_list['dashboard'][0] == 'covid_epid' :
                res = covid_epid(param_list)
            elif param_list['dashboard'][0] == 'covid_now' :
                res = covid_now(param_list)
            
            return JsonResponse(res, safe=False)
        else :
            return JsonResponse({}, safe=False)

def covid_now(param_list) :
    res = {}
    params_req = []
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
            res[ i['chart_name'] ] = i['chart_data']

    return res 

def covid_epid(param_list) :
    res = {}
    params_req = ['state']
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        state = param_list['state'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
            if i['chart_name'] in ['snapshot_table', 'snapshot_bar', 'bar_chart'] :
                res[ i['chart_name'] ] = i['chart_data']
            else :
                res[ i['chart_name'] ] = i['chart_data'][state]

    return res    

def covidvax(param_list) :
    res = {}
    params_req = ['state']
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        state = param_list['state'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
            if i['chart_name'] == 'snapshot_chart' :
                res[ i['chart_name'] ] = i['chart_data']
            else :
                res[ i['chart_name'] ] = i['chart_data'][state]

    return res

def blood_donation(param_list) :
    res = {}
    params_req = ['state']
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        state = param_list['state'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
            print(i['chart_name'])
            if i['chart_name'] in ['timeseries_facility', 'heatmap_bloodstock', 'map_facility'] :
                res[ i['chart_name'] ] = i['chart_data']
            else :
                res[ i['chart_name'] ] = i['chart_data'][state]

    return res

def slice_json_by_params(chart_params, url_params, data) :
    r_data = data

    for i in chart_params : 
        param_val = url_params[i][0]
        if param_val in data : 
            r_data = data[param_val]
        else : 
            break

    return r_data

def required_params(param_list, dict) : 
    for i in param_list : 
        if i not in dict : 
            return False
    
    return True

def default_params(param, dict, default) :
    if param not in dict :
        return default
    else :
        return dict[param][0]

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