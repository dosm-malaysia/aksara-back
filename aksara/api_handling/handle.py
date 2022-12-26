from aksara.models import MetaJson, KKMNowJSON, CatalogJson

'''
Build methods for any post-handling / additional info,
not covered by Meta Json
'''

def dashboard_additional_handling(params, res) :
    dashboard = params['dashboard'][0]

    if dashboard == 'homepage' : 
        catalog_count = CatalogJson.objects.all().count()  
        res['total_catalog'] = catalog_count
        return res
    else : # Default if no additions to make
        return res