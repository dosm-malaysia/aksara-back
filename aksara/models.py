import jsonfield
from django.db import models

class MetaJson(models.Model) :
    dashboard_name = models.CharField(max_length=200)
    dashboard_meta = models.JSONField()