import json

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from mergedeep import merge

from aksara.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil


class Pyramid(GeneralChartsUtil):
    """Pyramid Class for timeseries variables"""

    chart_type = "PYRAMID"

    # API related fields
    api_filter = []
    translations = {}

    # Chart related
    chart_name = {}
    p_keys = []
    p_x = ""
    p_y = []

    """
    Initiailize the neccessary data for a bar chart
    """

    def __init__(
        self,
        full_meta,
        file_data,
        meta_data,
        variable_data,
        all_variable_data,
        file_src,
    ):
        GeneralChartsUtil.__init__(
            self,
            full_meta,
            file_data,
            meta_data,
            variable_data,
            all_variable_data,
            file_src,
        )

        self.translations = (
            meta_data["chart"]["chart_variables"]["format_lang"]
            if "format_lang" in meta_data["chart"]["chart_variables"]
            else {}
        )
        self.chart_type = meta_data["chart"]["chart_type"]
        self.api_filter = meta_data["chart"]["chart_filters"]["SLICE_BY"]
        self.precision = (
            meta_data["chart"]["chart_filters"]["precision"]
            if "precision" in meta_data["chart"]["chart_filters"]
            else 1
        )

        self.api = self.build_api_info()

        self.p_keys = meta_data["chart"]["chart_variables"]["parents"]
        self.p_x = meta_data["chart"]["chart_variables"]["x"]
        self.p_y = meta_data["chart"]["chart_variables"]["y"]

        self.chart_name = {
            "en": self.variable_data["title_en"],
            "bm": self.variable_data["title_bm"],
        }

        self.metadata = self.rebuild_metadata()
        self.chart_details["chart"] = self.build_chart()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Build the pyramid chart
    """

    def build_chart(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        for key in self.p_keys:
            df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        res = {}
        tbl_res = {}
        overall = {}
        rename_columns = {self.p_x: "x", self.p_y[0]: "y1", self.p_y[1]: "y2"}

        if self.translations:
            tbl_res["tbl_columns"] = {
                "x_en": self.translations["x_en"],
                "x_bm": self.translations["x_bm"],
            }

            for y_lang in ["en", "bm"]:
                count = 1
                for c_y in self.translations["y_" + y_lang]:
                    y_val = "y" + str(count) + "_" + y_lang
                    tbl_res["tbl_columns"][y_val] = c_y
                    count += 1

        else:
            tbl_res["tbl_columns"] = {
                "x_en": self.p_x,
                "x_bm": self.p_x,
            }

            count = 1
            for c_y in self.p_y:
                for y_lang in ["en", "bm"]:
                    y_val = "y" + str(count) + "_" + y_lang
                    tbl_res["tbl_columns"][y_val] = c_y
                count += 1

        if len(self.p_keys) > 0:
            df["u_groups"] = list(df[self.p_keys].itertuples(index=False, name=None))
            u_groups_list = df["u_groups"].unique().tolist()

            for group in u_groups_list:
                result = {}
                tbl = {}
                for b in group[::-1]:
                    result = {b: result}
                    tbl = {b: tbl}
                group_l = [group[0]] if len(group) == 1 else list(group)
                group = group[0] if len(group) == 1 else group
                x_list = df.groupby(self.p_keys)[self.p_x].get_group(group).to_list()
                y1_list = [
                    x * -1
                    for x in df.groupby(self.p_keys)[self.p_y[0]]
                    .get_group(group)
                    .to_list()
                ]
                y2_list = (
                    df.groupby(self.p_keys)[self.p_y[1]].get_group(group).to_list()
                )

                table_vals = (
                    df.rename(columns=rename_columns)
                    .groupby(self.p_keys)[list(rename_columns.values())]
                    .get_group(group)
                    .to_dict("records")
                )

                chart_data = {"x": x_list, "y1": y1_list, "y2": y2_list}
                self.set_dict(result, group_l, chart_data)
                self.set_dict(tbl, group_l, table_vals)
                merge(res, result)
                merge(tbl_res, tbl)
        else:
            x_list = df[self.p_x].to_list()
            y1_list = [x * -1 for x in df[self.p_y[0]].to_list()]
            y2_list = df[self.p_y[1]].to_list()
            res = {"x": x_list, "y1": y1_list, "y2": y2_list}
            tbl_res = df.rename(columns=rename_columns)[
                list(rename_columns.values())
            ].to_dict("records")

        overall["chart_data"] = res
        overall["table_data"] = tbl_res
        return overall

    """
    Builds the API info for timeseries
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if self.api_filter:
            for api in self.api_filter:
                df[api] = df[api].astype(str)
                fe_vals = df[api].unique().tolist()
                be_vals = (
                    df[api]
                    .apply(lambda x: x.lower().replace(" ", "-"))
                    .unique()
                    .tolist()
                )
                api_obj = self.build_api_object_filter(
                    api, fe_vals[0], be_vals[0], dict(zip(fe_vals, be_vals))
                )
                api_filters_inc.append(api_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.meta_data["chart"]["chart_type"]

        return res["API"]

    """
    REBUILDS THE METADATA
    """

    def rebuild_metadata(self):
        self.metadata.pop("in_dataset", None)

        refresh_metadata = []

        for i in self.all_variable_data:
            if i["id"] != 0:
                i.pop("unique_id", None)
                refresh_metadata.append(i)

        self.metadata["out_dataset"] = refresh_metadata
        return self.metadata
