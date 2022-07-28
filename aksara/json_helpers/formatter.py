# All parameters of functions here, should be a dictionary
import json


'''
    FORMS OF DATA FORMATTING : 
    - METHOD
    - MAPPING
    - NONE
'''

def format_return_json(data) :
    r_format = data['format']
    r_data = data['data']

    r_val = {}

    for i in r_format : 
        if 'data-keys' in r_format[i] : 
            r_data = get_data_from_json(r_data, r_format[i]['data-keys'])

        val = operation_handler(r_format[i]['operation'], r_format[i]['variables'], r_data)
        r_val[i] = val

    return r_val

def operation_handler(operation, variables, data) :
    match operation :
        case 'MAPPING' :
            return mapping_handler(variables, data)
        case 'METHOD' :
            return method_handler(variables, data)

def mapping_handler(variables, data) :
    r_val = []
    data_type = type(data).__name__
    data = data if data_type == "list" else [data]

    for i in variables : 
        data_map = {"mys" : "country", "sgr" : "state", "ktn" : "state"} # Get the data map JSON here
        for v in data :
            r_val.append(data_map[v])

    return r_val if data_type == "list" else r_val[0]

def method_handler(variables, data) :
    r_val = ''

    for i in variables : 
        r_val = globals()[i](data)
    return r_val

def get_data_from_json(data, param_list) : 
    for p in param_list :
        if p in data :
            data = data[p]
        else : 
            break
    
    return data

def original_name(data) :
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

    r_val = []
    data_type = type(data).__name__
    data = data if data_type == "list" else [data]

    for i in data : 
        if i in STATE_ABBR :
            r_val.append(STATE_ABBR[i])
        else :
            HARD_CODED_AREAS = {
                'p.018-kulim-bandar-baharu' : 'P.018 Kulim-Bandar Baharu', 
                'n.27-layang-layang': 'N.27 Layang-Layang', 
                'n.20-api-api' : 'N.20 Api-Api', 
                'n.50-gum-gum' : 'N.50 Gum-Gum'}
            if data in HARD_CODED_AREAS :
                r_val.append(HARD_CODED_AREAS[i])
            else :
                r_val.append(i.replace("-", " ").title())

    return r_val if data_type == "list" else r_val[0]