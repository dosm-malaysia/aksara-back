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