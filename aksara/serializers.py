from rest_framework import serializers
from .models import MetaJson

class MetaSerializer(serializers.ModelSerializer) :
    class Meta : 
        model = MetaJson
        fields = ['dashboard_name', 'dashboard_meta']