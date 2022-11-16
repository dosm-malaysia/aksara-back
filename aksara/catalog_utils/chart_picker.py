from aksara.catalog_utils import chart_builder as cb

def build_chart(file, data) :
    chart_type = data['chart']['chart_type']

    if chart_type == 'TIMESERIES' : 
        file_link = file['link_parquet']
        variables = data['chart']['chart_variables']
        frequency = data['catalog_filters']['frequency']
        chart_name = file['variables'][data['id'] - 1]['name']
        return cb.timeseries_chart(file_link, variables, frequency, chart_name)        
