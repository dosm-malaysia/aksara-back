class GeneralChartsUtil : 
    # Data from file
    file_data = {}
    meta_data = {}
    variable_data = {}
    full_meta = {}
    all_variable_data = []
    
    # Identifiers
    unique_id = ''
    cur_id = 0
    result = {}
    db_input = {}
    variable_name = ''

    #file stuff
    read_from = ''

    #End results
    chart_details = {}
    explanation = {}
    metadata = {}
    downloads = {}
    api = {}
    
    '''
    Initiailize the general information for a catalog variable
    '''
    def __init__(self, full_meta, file_data, meta_data ,variable_data, all_variable_data) :
        self.file_data = file_data
        self.meta_data = meta_data
        self.variable_data = variable_data
        self.all_variable_data = all_variable_data
        self.full_meta = full_meta
        self.read_from  = file_data['link_parquet']

        self.cur_id = meta_data['id']
        self.unique_id = file_data['bucket'] + '_' + file_data['file_name'].replace(".parquet", "") + '_' + str(self.cur_id)
        self.variable_name = variable_data['name']
        self.db_input = self.build_db_info()

        self.explanation = self.meta_data['metadata_lang']
        self.metadata = self.build_metadata_info(self.file_data, self.meta_data, self.cur_id)
        self.downloads = self.build_downloads_info(self.file_data)
        self.chart_details = self.build_intro_info()

    '''
    Gets a nested dictionary key
    '''
    def get_dict(self, d, keys):
        for key in keys:
            d = d[key]
        return d

    '''
    Sets a nested dictionary key
    '''
    def set_dict(self, d, keys, value):
        d = self.get_dict(d, keys[:-1])
        d[keys[-1]] = value

    '''
    Builds the variables table for each catalog variable
    '''
    def build_variable_table(self, x_arr, y_arr) :
        res = []
        
        for i in range(0, len(x_arr)) :
            data = {'x' : x_arr[i], 'y' : y_arr[i]}
            res.append(data)

        return res

    '''
    Formats and builds, the metadata for each variable
    '''
    def build_metadata_info(self, file, data, cur_id) : 
        res = {}
        res['metadata'] = data['metadata_neutral']
        res['metadata']['dataset_desc'] = file['description']
        res['metadata']['data_source'] = data['catalog_filters']['data_source']
        res['metadata']['in_dataset'] = []
        res['metadata']['out_dataset'] = []
        res['metadata']['url'] = {}
        res['metadata']['url']['csv'] = file['link_csv']
        res['metadata']['url']['parquet'] = file['link_parquet']
        for v in self.all_variable_data :
            if v['id'] == cur_id : 
                v['unique_id'] = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(v['id'])
                res['metadata']['in_dataset'].append(v)
            else : 
                v['unique_id'] = file['bucket'] + '_' + file['file_name'].replace(".parquet", "") + '_' + str(v['id'])
                res['metadata']['out_dataset'].append(v)
        return res['metadata']

    '''
    Formats and builds, the downloads for each variable
    '''
    def build_downloads_info(self, file) : 
        res = {}
        res['downloads'] = {}
        res['downloads']['csv'] = file['link_csv']
        res['downloads']['parquet'] = file['link_parquet']
        return res['downloads']

    '''
    Helper to build the filters within an API
    '''
    def build_api_object_filter(self, key, def_lbl, def_val, options) : 
        # If it is a dictionary, convert to list of objects
        if isinstance(options, dict) : 
            options = [ {'label' : k, 'value' : v} for k, v in options.items() ]
        
        filter = {}
        filter['key'] = key
        filter['default'] = {}
        filter['default']['label'] = def_lbl
        filter['default']['value'] = def_val
        filter['options'] = options

        return filter

    '''
    Formats and builds, the intro for each variable
    '''
    def build_intro_info(self) :
        intro = {}
        intro['intro'] = {}
        intro['intro']['id'] = self.cur_id
        intro['intro']['unique_id'] = self.unique_id
        intro['intro']['name'] = self.variable_name

        for lang in ['en', 'bm'] : 
            intro['intro'][lang] = {}
            intro['intro'][lang]['title'] = self.variable_data['title_' + lang]
            intro['intro'][lang]['desc'] = self.variable_data['desc_' + lang]

        return intro

    '''
    Formats and builds, the database insertion for each variable
    '''
    def build_db_info(self) : 
        res = {}
        res['catalog_meta'] = self.full_meta
        res['catalog_name'] = self.variable_data['title_en'] + ' | ' + self.variable_data['title_bm']
        res['catalog_category'] = self.file_data['category']
        res['time_range'] = self.meta_data['catalog_filters']['frequency']
        res['geographic'] = ' | '.join(self.meta_data['catalog_filters']['geographic'])
        res['dataset_range'] = str( self.meta_data['catalog_filters']['start'] ) + '_' + str( self.meta_data['catalog_filters']['end'] )
        res['data_source'] = self.meta_data['catalog_filters']['data_source']

        return res

    '''
    Formats and builds, the overall catalog information for the API
    '''
    def build_catalog_data_info(self) : 
        res = {}
        res['API'] = self.api 
        res['explanation'] = self.explanation 
        res['metadata'] = self.meta_data
        res['downloads'] = self.downloads
        res['chart_details'] = self.chart_details

        return res
