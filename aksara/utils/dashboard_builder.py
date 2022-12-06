from aksara.utils.general_chart_helpers import *
from aksara.utils.chart_builder import *
import os

'''
Segregates chart types,
into respective chart builders
'''

def build_chart(chart_type, data) :
    variables = data['variables']
    # input_file = os.path.join(os.getcwd(), 'KKMNOW_SRC/kkmnow-data-main') + '/' + data['input']
    input_file = data['input']

    if chart_type == 'bar_chart':
        return bar_chart(input_file, variables)
    elif chart_type == 'heatmap_chart':
        return heatmap_chart(input_file, variables)
    elif chart_type == 'timeseries_chart':  
        return timeseries_chart(input_file, variables)
    elif chart_type == 'bar_meter':
        return bar_meter(input_file, variables)
    elif chart_type == 'custom_chart':
        return custom_chart(input_file, variables)
    elif chart_type == 'snapshot_chart':
        return snapshot_chart(input_file, variables)
    elif chart_type == 'waffle_chart':
        return waffle_chart(input_file, variables)
    elif chart_type == 'helpers_custom':
        return helpers_custom(input_file)
    elif chart_type == 'map_lat_lon':  
        return map_lat_lon(input_file, variables)
    elif chart_type == 'choropleth_chart' : 
        return choropleth_chart(input_file, variables)
    else:
        return {}