from aksara.utils.general_chart_helpers import *
from aksara.utils.chart_builder import *
import os

def build_chart(chart_type, data) :
    cwd = os.getcwd()
    variables = data['variables']
    input_file = data['input'].replace("./", '')
    f_name = os.path.dirname(os.path.realpath(__file__)) + input_file
    input_file = f_name

    match chart_type : 
        case 'bar_chart' :
            return bar_chart(input_file, variables)
        case 'heatmap_chart' :
            return heatmap_chart(input_file, variables)
        case 'timeseries_chart' : 
            return timeseries_chart(input_file, variables)
        case 'bar_meter' :
            return bar_meter(input_file, variables)
        case 'custom_chart' :
            return custom_chart(input_file, variables)
        case 'snapshot_chart' :
            return snapshot_chart(input_file, variables)
        case 'waffle_chart' :
            return waffle_chart(input_file, variables)
        case 'helpers_custom' :
            return helpers_custom(input_file)
        case _:
            # If its not found
            return {}