import datetime
import json
import logging
import os
import sys
import uuid
from datetime import date, timedelta
from platform import version

import pandas as pd
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlockBlobService, ContentSettings
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from pandas.core.frame import DataFrame
from pandas.io.json import json_normalize

from .fields import (ACTION_TYPE_LIST, FIELDS, FIELDS_FOR_ADS,
                     FIELDS_FOR_CAMPAIGNS, NEW_COL_NAME)


class fbConnectorForAzure():
    def __init__(
            self,
            date_to_query,
            ad_account,
            access_token,
            app_id,
            app_secret,
            account_url,
            container_name,
            file_path,
            file_name,
    ):
        self.date_to_query = date_to_query
        self.ad_account = ad_account
        self.access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.all_campaign_ids = []
        self.active_campaign_ids = []
        self.df = None
        self.df_to_upload = None
        self.account_url = account_url
        self.container_name = container_name
        self.file_path = file_path
        self.file_name = f'{file_name}.csv'

    def run(self):
        self.authenticate()
        self.get_active_campaign_ids()
        self.get_ad_data()
        self.transform()
        self.upload_with_default_azure_credential()

    def authenticate(self):
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        print("Authentication successful")

    def get_active_campaign_ids(self):
        get_ids = AdAccount(self.ad_account).get_campaigns()
        for i in get_ids:
            data = dict(i)
            self.all_campaign_ids.append(data["id"])

        campaign_id_df = self.__call_api(
            "campaign", FIELDS_FOR_CAMPAIGNS, self.all_campaign_ids)

        self.active_campaign_ids = pd.concat(campaign_id_df).reset_index(
            drop=True).campaign_id.to_list()

        print("Get active campaign ids successful")

    def get_ad_data(self):
        df_list = self.__call_api(
            "ad", FIELDS_FOR_ADS, self.active_campaign_ids)

        self.df = pd.concat(df_list).reset_index(
            drop=True)

        print("Get ad data successful")

    def transform(self):

        self.df = self.df.fillna(0)

        # splits out actions column metrics into seperate columns
        for action_type in ACTION_TYPE_LIST:
            value = self.__flatten_actions(self.df, action_type)
            self.df[action_type] = value

        # if field not in df then add it in
        for k in FIELDS + NEW_COL_NAME:
            if k not in self.df:
                self.df[k] = 0

        print("Transform successful")

    def upload_with_default_azure_credential(self):
        self.df_to_upload = self.df
        output = self.df_to_upload.applymap(lambda x: x[0].get("value") if isinstance(
            x, list) is True and len(x) == 1 else x).fillna(0).to_csv(index=False)

        credential = DefaultAzureCredential()
        blob_client = BlobClient(
            self.account_url, self.container_name, self.file_path + self.file_name, credential)
        blob_client.upload_blob(output)

        return

    def upload_with_sas(self):
        # upload using an azure blob sas token
        self.df_to_upload = self.df

        output = self.df_to_upload.applymap(
            lambda x: x[0].get("value") if isinstance(
                x, list) is True and len(x) == 1 else x).fillna(0).to_csv(
            index=False)

        sas_url = self.account_url + self.file_path + \
            self.file_name + '?' + self.sas_token
        encoded_data = output.encode("utf-8")
        content_type_string = ContentSettings()
        r = requests.put(sas_url,
                         data=encoded_data,
                         headers={
                             'content-type': content_type_string.content_type,
                             'x-ms-blob-type': 'BlockBlob'
                         }
                         )
        return print(r.text)

    def __call_api(self, granularity, fields_string, campaign_ids):
        if granularity == "campaign":
            params = (
                # to use time range instead of date present remove the comments
                # and comment in the date preset
                ('time_range[since]', self.date_to_query),
                ('time_range[until]', self.date_to_query),
                #('date_preset', 'yesterday'),
                ('time_increment', '1'),
                ('level', granularity),
                ('fields', fields_string),
                ('access_token', self.access_token),
            )
        else:
            params = (
                # to use time range instead of date present remove the comments
                # and comment in the date preset
                ('time_range[since]', self.date_to_query),
                ('time_range[until]', self.date_to_query),
                #('date_preset', 'yesterday'),
                ('time_increment', '1'),
                ('level', granularity),
                ('fields', fields_string),
                ('breakdowns', 'publisher_platform, platform_position, impression_device'),
                ('access_token', self.access_token),
                ('action_attribution_windows',
                 '[\'1d_click\',\'7d_click\',\'28d_click\',\'1d_view\', \'7d_view\', \'28d_view\', \'dda\']')
            )

        df = []
        try:
            for i in range(len(campaign_ids)):

                url = 'https://graph.facebook.com/v10.0/{}/insights'.format(
                    campaign_ids[i])
                response = requests.get(url, params=params)
                json_obj = json.loads(response.text)
                df.append(json_normalize(json_obj["data"]))

                try:
                    while ((json_obj["paging"] is not None) and (
                            json_obj["paging"]["next"] is not None)):
                        response = requests.get(json_obj["paging"]["next"])
                        json_obj = json.loads(response.text)
                        df.append(json_normalize(json_obj["data"]))
                except KeyError:
                    pass

        except Exception:
            print(response.text)

        print("Api call successful")
        return df

    def __flatten_actions(self, df, action_type):
        value = []
        for action in df["actions"]:

            # if action row is 0 then append 0
            # df.fillna(0) should have been used before this
            if action == 0:
                value.append(0)

            # if there is not action type in i then append 0
            elif not any(i["action_type"] == action_type for i in action):
                value.append(0)

            # if action type in i then append i["value"]
            # if only 1d_view etc. is in dict then
            # append 0
            else:
                for i in action:
                    if i["action_type"] == action_type:
                        if i.get("value"):
                            value.append(i["value"])
                        else:
                            value.append(0)

        return value
