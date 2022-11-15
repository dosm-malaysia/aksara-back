import jsonfield
from django.db import models

class MetaJson(models.Model) :
    dashboard_name = models.CharField(max_length=200)
    dashboard_meta = models.JSONField()

class KKMNowJSON(models.Model) :
    dashboard_name = models.CharField(max_length=200)
    chart_name = models.CharField(max_length=200, null=True)
    chart_type = models.CharField(max_length=200, null=True)
    api_type = models.CharField(max_length=200, null=True)
    chart_data = models.JSONField()

class CatalogJson(models.Model) :
    id = models.CharField(max_length=400, primary_key=True)
    catalog_meta = models.JSONField()
    catalog_name = models.CharField(max_length=400)
    catalog_category = models.CharField(max_length=300)
    time_range = models.CharField(max_length=100)
    geographic = models.CharField(max_length=300)
    dataset_range = models.CharField(max_length=100)
    data_source = models.CharField(max_length=100)
    catalog_data = models.JSONField()
     
