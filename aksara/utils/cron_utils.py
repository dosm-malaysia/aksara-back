import json
import os
import shutil
import zipfile
from os import listdir
from os.path import isfile, join

import requests
from django.apps import apps
from django.conf import settings
from django.core.cache import cache
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from aksara.api_handling import cache_search
from aksara.catalog_utils import catalog_builder
from aksara.models import CatalogJson, DashboardJson, MetaJson
from aksara.utils import common, data_utils, triggers


def create_directory(dir_name):
    """Creates a directory."""
    try:
        os.mkdir(os.path.join(os.getcwd(), dir_name))
    except OSError as e:
        print(f"Directory already exists, no need to create: {e}")


def fetch_from_git(zip_name, git_url, git_token):
    """Fetches entire content from a git repo."""
    file_name = os.path.join(os.getcwd(), zip_name)
    headers = {
        "Authorization": f"token {git_token}",
        "Accept": "application/vnd.github.v3.raw",
    }

    res = {}
    res["file_name"] = file_name
    res["data"] = requests.get(git_url, headers=headers)
    res["resp_code"] = res["data"].status_code
    return res


def write_as_binary(file_name, data):
    """Writes content as binary."""
    try:
        with open(file_name, "wb") as f:
            f.write(data.content)
    except Exception as e:
        triggers.send_telegram(f"!! FILE ISSUES WRITING TO BINARY: {e} !!")


def extract_zip(file_name, dir_name):
    """Extracts zip file into desired directory."""
    try:
        with zipfile.ZipFile(file_name, "r") as zip_ref:
            zip_ref.extractall(os.path.join(os.getcwd(), dir_name))
    except Exception as e:
        triggers.send_telegram(f"!! ZIP FILE EXTRACTION ISSUE: {e} !!")


def data_operation(operation, op_method):
    """Performs data operations, such as update or rebuild."""
    dir_name = "AKSARA_SRC"
    zip_name = "repo.zip"
    git_url = os.getenv("GITHUB_URL", "-")
    git_token = os.getenv("GITHUB_TOKEN", "-")

    triggers.send_telegram("--- PERFORMING " + op_method + " " + operation + " ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url, git_token)
    if "resp_code" in res and res["resp_code"] == 200:
        write_as_binary(res["file_name"], res["data"])
        extract_zip(res["file_name"], dir_name)
        data_utils.rebuild_dashboard_meta(operation, op_method)
        data_utils.rebuild_dashboard_charts(operation, op_method)
    else:
        triggers.send_telegram("FAILED TO GET SOURCE DATA")


def get_latest_info_git(type, commit_id):
    """get_latest_info_git.

    Args:
        type: type
        commit_id: commit_id
    """
    sha_ext = os.getenv("GITHUB_SHA_URL", "-")
    url = "https://api.github.com/repos/dosm-malaysia/aksara-data/commits/" + sha_ext
    headers_accept = "application/vnd.github.VERSION.sha"

    git_token = os.getenv("GITHUB_TOKEN", "-")

    if type == "COMMIT":
        url = url.replace(sha_ext, "")
        url += commit_id
        headers_accept = "application/vnd.github+json"

    res = requests.get(
        url, headers={"Authorization": f"token {git_token}", "Accept": headers_accept}
    )

    if res.status_code == 200:
        return str(res.content, "UTF-8")
    else:
        triggers.send_telegram("!!! FAILED TO GET GITHUB " + type + " !!!")


def selective_update():
    """selective_update."""
    # Delete all file src
    # os.remove("repo.zip")
    # shutil.rmtree("AKSARA_SRC/")
    remove_src_folders()

    dir_name = "AKSARA_SRC"
    zip_name = "repo.zip"
    git_url = os.getenv("GITHUB_URL", "-")
    git_token = os.getenv("GITHUB_TOKEN", "-")

    triggers.send_telegram("--- PERFORMING SELECTIVE UPDATE ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url, git_token)
    if "resp_code" in res and res["resp_code"] == 200:
        write_as_binary(res["file_name"], res["data"])
        extract_zip(res["file_name"], dir_name)

        latest_sha = get_latest_info_git("SHA", "")
        data = json.loads(get_latest_info_git("COMMIT", latest_sha))
        changed_files = [f["filename"] for f in data["files"]]
        filtered_changes = filter_changed_files(changed_files)

        remove_deleted_files()

        if filtered_changes["dashboards"]:
            fin_files = [x.replace(".json", "") for x in filtered_changes["dashboards"]]
            file_list = ",".join(fin_files)

            triggers.send_telegram("Updating : " + file_list)

            operation = "UPDATE " + file_list
            data_utils.rebuild_dashboard_meta(operation, "AUTO")
            validate_info = data_utils.rebuild_dashboard_charts(operation, "AUTO")

            dashboards_validate = validate_info["dashboard_list"]
            failed_dashboards = validate_info["failed_dashboards"]

            # Validate each dashboard
            dashboards_validate_status = []

            for dbd in dashboards_validate:
                if dbd not in failed_dashboards:
                    if revalidate_frontend(dbd) == 200:
                        dashboards_validate_status.append(
                            {"status": "✅", "variable": dbd}
                        )
                    else:
                        dashboards_validate_status.append(
                            {"status": "❌", "variable": dbd}
                        )
                else:
                    dashboards_validate_status.append({"status": "❌", "variable": dbd})

            if dashboards_validate_status:
                revalidation_results = triggers.format_status_message(
                    dashboards_validate_status, "-- DASHBOARD REVALIDATION STATUS --"
                )
                triggers.send_telegram(revalidation_results)

        if filtered_changes["catalog"]:
            fin_files = [x.replace(".json", "") for x in filtered_changes["catalog"]]
            file_list = ",".join(fin_files)
            operation = "UPDATE " + file_list
            catalog_builder.catalog_update(operation, "AUTO")

            # Update Cache Here
            source_filters_cache()
            catalog_list = list(
                CatalogJson.objects.all().values(
                    "id",
                    "catalog_name",
                    "catalog_category",
                    "catalog_category_name",
                    "catalog_subcategory_name",
                )
            )
            cache.set("catalog_list", catalog_list)
            # cache_search.set_filter_cache()
    else:
        triggers.send_telegram("FAILED TO GET SOURCE DATA")


def filter_changed_files(file_list):
    """Filters the changed files for dashboards and catalog data.

    Args:
        file_list: file_list
    """
    changed_files = {"dashboards": [], "catalog": []}

    for f in file_list:
        f_path = "AKSARA_SRC/" + os.getenv("GITHUB_DIR", "-") + "/" + f
        f_info = f.split("/")
        if len(f_info) > 1 and f_info[0] in changed_files and os.path.exists(f_path):
            changed_files[f_info[0]].append(f_info[1])

    return changed_files


def remove_deleted_files():
    """remove_deleted_files."""
    for k, v in common.REFRESH_VARIABLES.items():
        model_name = apps.get_model("aksara", k)
        distinct_db = [
            m[v["column_name"]]
            for m in model_name.objects.values(v["column_name"]).distinct()
        ]
        DIR = os.path.join(
            os.getcwd(), "AKSARA_SRC/" + os.getenv("GITHUB_DIR", "-") + v["directory"]
        )
        distinct_dir = [
            f.replace(".json", "") for f in listdir(DIR) if isfile(join(DIR, f))
        ]
        diff = list(set(distinct_db) - set(distinct_dir))

        if diff:
            # Remove the deleted datasets
            query = {v["column_name"] + "__in": diff}
            model_name.objects.filter(**query).delete()

    # Update the cache
    source_filters_cache()
    catalog_list = list(
        CatalogJson.objects.all().values(
            "id",
            "catalog_name",
            "catalog_category",
            "catalog_category_name",
            "catalog_subcategory_name",
        )
    )
    cache.set("catalog_list", catalog_list)


def revalidate_frontend(dashboard):
    """revalidate_frontend.

    Args:
        dashboard: dashboard
    """
    if dashboard not in common.FRONTEND_ENDPOINTS:
        return -1

    endpoint = common.FRONTEND_ENDPOINTS[dashboard]
    url = os.getenv("FRONTEND_URL", "-")
    fe_auth = os.getenv("FRONTEND_REBUILD_AUTH", "-")

    headers = {"Authorization": fe_auth}
    body = {"route": endpoint}

    response = requests.post(url, headers=headers, data=body)

    return response.status_code


def source_filters_cache():
    """Set Source Filters Cache."""
    filter_sources_distinct = CatalogJson.objects.values("data_source").distinct()
    source_filters = set()

    for x in filter_sources_distinct:
        if "|" in x["data_source"]:
            sources = x["data_source"].split(" | ")
            for s in sources:
                source_filters.add(s)
        else:
            source_filters.add(x["data_source"])

    cache.set("source_filters", list(source_filters))

    return list(source_filters)


def remove_src_folders():
    """remove_src_folders."""
    if os.path.exists("AKSARA_SRC") and os.path.isdir("AKSARA_SRC"):
        shutil.rmtree("AKSARA_SRC")
    if os.path.exists("repo.zip"):
        os.remove("repo.zip")
