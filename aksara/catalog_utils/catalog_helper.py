from aksara.catalog_utils import chart_utils as cu

def build_metadata_key(file, data, cur_id) : 
    res = {}

    res['metadata'] = data['metadata_neutral']
    res['metadata']['dataset_desc'] = file['description']
    res['metadata']['data_source'] = data['catalog_filters']['data_source']
    res['metadata']['in_dataset'] = []
    res['metadata']['out_dataset'] = []
    res['metadata']['url'] = {}
    res['metadata']['url']['csv'] = file['link_csv']
    res['metadata']['url']['parquet'] = file['link_parquet']
    for v in file['variables'] :
        if v['id'] == cur_id : 
            v['unique_id'] = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(v['id'])
            res['metadata']['in_dataset'].append(v)
        else : 
            v['unique_id'] = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(v['id'])
            res['metadata']['out_dataset'].append(v)

    return res['metadata']

def additional_info(file, data, chart_type, res) : 
    if chart_type == 'TIMESERIES' :
        timeseries_vals = {
            'DAILY' : 'Daily',
            'WEEKLY' : 'Weekly',
            'MONTHLY' : 'Monthly',
            'YEARLY' : 'Yearly'
        }

        pos_vals = cu.get_range_values(data['catalog_filters']['frequency'])
        options = []
        for p in pos_vals : 
            options.append({'label' : timeseries_vals[p], 'value' : p})

        filter = {
            'key' : 'range',
            'default' : {
                'label' : timeseries_vals[ data['catalog_filters']['frequency'] ],
                'value' : data['catalog_filters']['frequency']
            },
            'options' : options
        }
        res.append(filter)


def format_intro(intro) : 
    intro_frmtted = {
        'id' : intro['id'],
        'name' : intro['name'],
        'en' : {
            'title' : intro['title_en'],
            'desc' : intro['desc_en']
        },
        'bm' : {
            'title' : intro['title_bm'],
            'desc' : intro['desc_bm']
        }
    }

    return intro_frmtted