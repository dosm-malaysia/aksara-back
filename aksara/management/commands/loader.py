from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from aksara.utils import cron_utils
from aksara.catalog_utils import catalog_builder

from django.core.cache import cache

import environ

env = environ.Env()
environ.Env.read_env()

class Command(BaseCommand):
    def add_arguments(self , parser):
        parser.add_argument('operation' , nargs='+' , type=str, 
        help='States what the operation should be') 
    
    def handle(self, *args, **kwargs):
        category = kwargs['operation'][0]
        operation =  kwargs['operation'][1]

        if len(kwargs['operation']) > 2 : 
            files = kwargs['operation'][2]
            operation = operation + " " + files
        
        '''
        CATEGORIES : 
        1. DATA_CATALOG
        2. DASHBOARD

        OPERATIONS : 
        1. UPDATE
            - Updates the db, by updating values of pre-existing records
        
        2. REBUILD
            - Rebuilds the db, by clearing existing values, and inputting new ones

        SAMPLE COMMAND : 
        - python manage.py loader DATA_CATALOG REBUILD
        - python manage.py loader DASHBOARDS UPDATE meta_1,meta_2
        '''

        if category == 'DATA_CATALOG' : 
            catalog_builder.catalog_operation(operation, "MANUAL")
        else : 
            cron_utils.data_operation(operation, "MANUAL")
