from django.http import JsonResponse
from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache

from .serializers import MetaSerializer, KKMSerializer
from .models import MetaJson, KKMNowJSON

import json

class KKMNOW(APIView) :
    def get(self, request, format=None):
        param_list = dict(request.GET)
        params_req = ['dashboard']
        
        if all (p in param_list for p in params_req) :
            res = {}

            '''
            If/else below is temporary, using now only to work development faster.
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
            elif param_list['dashboard'][0] == 'facilities' :                
                res = facilities(param_list)
            elif param_list['dashboard'][0] == 'organ_donation' :
                res = organ_donation(param_list)
            elif param_list['dashboard'][0] == 'peka_b40' :
                res = peka_b40(param_list)        

            return JsonResponse(res, safe=False)
        else :
            return JsonResponse({}, safe=False)

def facilities(param_list) :
    res = {}
    params_req = []
    params_opt = []
    
    # 1. Table returns by default, doesn't return if table is set to False 

    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        fac_type = '' if 'fac_type' not in param_list else param_list['fac_type'][0]
        district = '' if 'district' not in param_list else param_list['district'][0]
        state = '' if 'state' not in param_list else param_list['state'][0]
        table = 'true' if 'table' not in param_list else param_list['table'][0]
        table = True if table == 'true' else False 

        print(table)

        chart_params = {
            'locations_mapping' : [state, district, fac_type],
            'distances_within' : [fac_type, state, district],
            'distances_between' : [fac_type, 'district']
        }

        for i in info:
            if i['chart_name'] in [ 'facilities_table', 'helpers' ] :
                res[ i['chart_name'] ] = i['chart_data']
            else :
                if fac_type != '' and state != '' and district != '': # Facilty type is needed
                    values = chart_params[ i['chart_name'] ]
                    temp = i['chart_data']
                    for x in values : 
                        temp = temp[x]
                    res[ i['chart_name'] ] = temp

        if not table:
            res.pop('facilities_table')

    return res 

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

def organ_donation(param_list) :
    res = {}
    params_req = ['state']
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        state = param_list['state'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
            res[ i['chart_name'] ] = i['chart_data'][state]

    return res

def peka_b40(param_list) :
    res = {}
    params_req = ['state']
    
    if all (p in param_list for p in params_req) :
        dbd_name = param_list['dashboard'][0]
        state = param_list['state'][0]
        info = KKMNowJSON.objects.filter(dashboard_name=dbd_name).values()

        for i in info:
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