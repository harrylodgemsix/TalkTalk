import datetime
import json
import logging
import os
import sys
import uuid
from datetime import date, timedelta
from platform import version
from urllib.request import urlopen

import httplib2
import pandas as pd
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import (BlobClient, BlockBlobService, ContentSettings,
                                PublicAccess)
from google.oauth2 import service_account
from googleapiclient import discovery, http
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from oauth2client.service_account import ServiceAccountCredentials
from pandas.core.frame import DataFrame
from pandas.io.json import json_normalize

API_SCOPES = ['https://www.googleapis.com/auth/doubleclickbidmanager']


class dv360ConnectorForAzure():
    def __init__(self):
        self.service_account_json_dict = None
        self.report_id = None
        self.report_definition = None
        self.dbm_service = None

    def authenticate(self, service_account_json_dict):
        self.service_account_json_dict = service_account_json_dict
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.service_account_json_dict,
            scopes=API_SCOPES)

        http = credentials.authorize(httplib2.Http())
        self.dbm_service = discovery.build(
            "doubleclickbidmanager", "v1.1", http=http)

    def create_report(self, report_definition):
        self.report_definition = report_definition
        report = self.dbm_service.queries().createquery(
            body=self.report_definition).execute()
        self.report_id = report["queryId"]

    def download_and_upload_report(self, report_id, account_url, container_name, file_path, file_name):
        self.report_id = report_id

        getquery = (self.dbm_service.queries().getquery(
            queryId=self.report_id).execute())

        df = pd.read_csv(getquery["metadata"]
                         ["googleCloudStoragePathForLatestReport"], dtype={"Insertion Order ID": "str", "Line Item ID": "str", "YouTube Ad Group ID": "str"})
        df = df[:-20]
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%d/%m/%G")

        output = df.to_csv(index=False)

        self.__upload_with_default_azure_credential(
            account_url, container_name, file_path, file_name, output)

    def __upload_with_default_azure_credential(self, account_url, container_name, file_path, file_name, data):
        credential = DefaultAzureCredential()
        blob_client = BlobClient(
            account_url, container_name, file_path + file_name, credential)
        blob_client.upload_blob(data)

    def __upload_with_sas(account_url, file_path, file_name, sas_token, data):
        yesterday_date = date.today() - timedelta(days=1)
        sas_url = account_url + file_path + file_name + '?' + sas_token
        encoded_data = data.encode("utf-8")
        content_type_string = ContentSettings()
        r = requests.put(sas_url,
                         data=encoded_data,
                         headers={
                             'content-type': content_type_string.content_type,
                             'x-ms-blob-type': 'BlockBlob'
                         }
                         )
        return print(r.text)
