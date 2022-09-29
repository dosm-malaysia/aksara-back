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