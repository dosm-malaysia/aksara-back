from django.contrib import admin

from .models import DashboardJson, MetaJson

admin.site.register(MetaJson)
admin.site.register(DashboardJson)
