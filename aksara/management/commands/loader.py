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
        operation =  kwargs['operation'][0]

        if len(kwargs['operation']) > 1 : 
            files = kwargs['operation'][1]
            operation = operation + " " + files

        '''
        OPERATIONS : 
        1. UPDATE
            - Updates the db, by updating values of pre-existing records
        
        2. REBUILD
            - Rebuilds the db, by clearing existing values, and inputting new ones
        '''

        if operation == 'DATA_CATALOG' : 
            catalog_builder.test_build()
        else : 
            cron_utils.data_operation(operation)
