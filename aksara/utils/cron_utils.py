import os
from os import listdir
from os.path import isfile, join
from wsgiref import validate
from aksara.models import MetaJson, KKMNowJSON
from aksara.utils import triggers
from aksara.utils import data_utils
from aksara.utils import common
import requests
import zipfile
import json

'''
Creates a directory
'''
def create_directory(dir_name) :
    try: 
        os.mkdir(os.path.join(os.getcwd(), dir_name)) # KKMNOW_SRC
    except OSError as error: 
        print("Directory already exists, no need to create")

'''
Fetches entire content from a git repo
'''
def fetch_from_git(zip_name, git_url, git_token) :
    file_name = os.path.join(os.getcwd(), zip_name)
    headers = {
        'Authorization': f'token {git_token}',
        'Accept': 'application/vnd.github.v3.raw'
    }

    res = {}
    res['file_name'] = file_name
    res['data'] = requests.get(git_url, headers=headers)
    res['resp_code'] = res['data'].status_code
    return res

'''
Writes content as binary
'''
def write_as_binary(file_name, data) :
    try : 
        with open(file_name, 'wb') as f:
            f.write(data.content)
    except : 
        triggers.send_telegram("!! FILE ISSUES WRITING TO BINARY !!")

'''
Extracts zip file into desired directory
'''
def extract_zip(file_name, dir_name) :
    try : 
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(os.getcwd(), dir_name))
    except : 
        triggers.send_telegram("!! ZIP FILE EXTRACTION ISSUE !!")

'''
Performs data operations,
such as update or rebuild
'''
def data_operation(operation) :
    dir_name = 'KKMNOW_SRC'
    zip_name = 'repo.zip'
    git_url = 'https://github.com/MoH-Malaysia/kkmnow-data/archive/main.zip'
    git_token = os.getenv('GITHUB_TOKEN', '-')

    triggers.send_telegram("--- PERFORMING " + operation + " ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url, git_token)
    if 'resp_code' in res and res['resp_code'] == 200 : 
        write_as_binary(res['file_name'], res['data'])
        extract_zip(res['file_name'], dir_name)
        data_utils.rebuild_dashboard_meta(operation)
        data_utils.rebuild_dashboard_charts(operation)
    else : 
        triggers.send_telegram("FAILED TO GET SOURCE DATA")

def get_latest_info_git(type, commit_id) : 
    url = "https://api.github.com/repos/MoH-Malaysia/kkmnow-data/commits/main"
    headers_accept = 'application/vnd.github.VERSION.sha'

    git_token = os.getenv('GITHUB_TOKEN', '-')

    if type == 'COMMIT' : 
        url = url.replace('main', '')
        url += commit_id
        headers_accept = 'application/vnd.github+json'

    res = requests.get(url, headers={'Authorization': f'token {git_token}','Accept': headers_accept})

    if res.status_code == 200 : 
        return str(res.content, 'UTF-8')
    else : 
        triggers.send_telegram("!!! FAILED TO GET GITHUB " + type + " !!!")

def selective_update() :
    dir_name = 'KKMNOW_SRC'
    zip_name = 'repo.zip'
    git_url = 'https://github.com/MoH-Malaysia/kkmnow-data/archive/main.zip'
    git_token = os.getenv('GITHUB_TOKEN', '-')

    triggers.send_telegram("--- PERFORMING SELECTIVE UPDATE ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url, git_token)
    if 'resp_code' in res and res['resp_code'] == 200 : 
        write_as_binary(res['file_name'], res['data'])
        extract_zip(res['file_name'], dir_name)
        
        latest_sha = get_latest_info_git('SHA', '')
        data = json.loads(get_latest_info_git('COMMIT', latest_sha))
        changed_files = [ f['filename'] for f in data['files']]        
        
        validate_info = data_utils.rebuild_selective_update(changed_files)
        dashboards_validate = validate_info['dashboard_list']
        failed_dashboards = validate_info['failed_dashboards']
        
        for dbd in dashboards_validate : 
            if dbd not in failed_dashboards : 
                revalidate_frontend(dbd)
            else : 
                triggers.send_telegram("Validation for " + dbd + " : " + " not sent.")

    else : 
        triggers.send_telegram("FAILED TO GET SOURCE DATA")

def revalidate_frontend(dashboard) :
    endpoint = common.FRONTEND_ENDPOINTS[dashboard]
    url = os.getenv('FRONTEND_URL', '-')
    fe_auth = os.getenv('FRONTEND_REBUILD_AUTH', '-')
    
    headers = {'Authorization' : fe_auth}
    body = {'route' : endpoint}

    response = requests.post(url, headers=headers, data=body)

    if response.status_code == 200 : 
        triggers.send_telegram(dashboard + " page, successfully validated.")
    else : 
        triggers.send_telegram(dashboard + " page, failed to validated.")