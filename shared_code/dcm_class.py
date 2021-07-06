import glob
import io
import logging
import os
import random
import sys
import time
from datetime import date, timedelta
from io import StringIO
from os import path
from urllib.request import urlopen

import httplib2
import pandas as pd
import requests as rq
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import (BlobClient, BlockBlobService, ContentSettings,
                                PublicAccess)
from google.oauth2 import service_account
from googleapiclient import discovery, http
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize

KVUri = os.environ["KVUri"]
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

BLOB_ACCESS_KEY_SECRET = client.get_secret("blob-account-key")
BLOB_ACCESS_KEY_VALUE = BLOB_ACCESS_KEY_SECRET.value

ACCOUNT_URL_SECRET = client.get_secret("blob-account-url")
ACCOUNT_URL = ACCOUNT_URL_SECRET.value

SAS_SECRET_NAME = os.environ["integration_sas_secret_all"]
SAS_TOKEN_SECRET = client.get_secret(SAS_SECRET_NAME)
SAS_TOKEN = SAS_TOKEN_SECRET.value


class dcmConnectorForAzure():
    def __init__(self, dcm_account_id, service_account_json_keyfile_dict):
        self.dcm_account_id = dcm_account_id
        self.json_keyfile_dict = service_account_json_keyfile_dict
        self.profile_id = None
        self.report_id = None

    def authenticate_using_service_account(self):
        #path_to_service_account_json_file = os.path.abspath(service_account_json_file)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.json_keyfile_dict, scopes=['https://www.googleapis.com/auth/dfareporting'])

        self.http = credentials.authorize(httplib2.Http())

        self.service = discovery.build('dfareporting', 'v3.4', http=self.http)
        service = self.service

        request = service.userProfiles().list()

        response = request.execute()

        for profile in response['items']:
            if profile["accountId"] == str(self.dcm_account_id):
                self.profile_id = profile["profileId"]

    def create_and_run_report(self, start_date, end_date, metric_names, dimension_names):
        service = self.service
        report = {
            'name': 'dcm_daily_report',
            'type': 'STANDARD',
            'fileName': 'dcm_daily_report',
            'format': 'CSV',
            'accountId': self.dcm_account_id
        }

        criteria = {
            'dateRange': {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d')
            },
            'dimensions': dimension_names,
            'metricNames': metric_names
        }

        report["criteria"] = criteria

        inserted_report = service.reports().insert(
            profileId=self.profile_id, body=report).execute()

        self.report_id = inserted_report['id']

        report_file = service.reports().run(profileId=self.profile_id,
                                            reportId=self.report_id).execute()

    def create_and_run_reach_report(self, start_date, end_date, metric_names, dimension_names):
        service = self.service
        report = {
            'name': 'dcm_daily_reach_report',
            'type': 'REACH',
            'fileName': 'dcm_daily_reach_report',
            'format': 'CSV',
            'accountId': self.dcm_account_id
        }

        reachCriteria = {
            'dateRange': {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d')
            },
            'dimensions': dimension_names,
            'metricNames': metric_names
        }

        report["reachCriteria"] = reachCriteria

        inserted_report = service.reports().insert(
            profileId=self.profile_id, body=report).execute()

        self.report_id = inserted_report['id']

        report_file = service.reports().run(profileId=self.profile_id,
                                            reportId=self.report_id).execute()

    def create_and_run_floodlight_report(self, start_date, end_date, metric_names, dimension_names, floodlightConfigId):
        service = self.service
        report = {
            'name': 'dcm_daily_floodlight_report',
            'type': 'FLOODLIGHT',
            'fileName': 'dcm_daily_floodlight_report',
            'format': 'CSV',
            'accountId': self.dcm_account_id
        }

        floodlightCriteria = {
            'dateRange': {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d')
            },
            'floodlightConfigId': {
                'dimensionName': "dfa:floodlightConfigId",
                'value': floodlightConfigId
            },
            'dimensions': dimension_names,
            'metricNames': metric_names
        }

        report["floodlightCriteria"] = floodlightCriteria

        inserted_report = service.reports().insert(
            profileId=self.profile_id, body=report).execute()

        self.report_id = inserted_report['id']

        report_file = service.reports().run(profileId=self.profile_id,
                                            reportId=self.report_id).execute()

    def download_report_upload_to_azure(self, report_id, account_url, container_name, file_path, file_name):
        service = self.service
        request = service.reports().files().list(
            profileId=self.profile_id, reportId=report_id).execute()
        file_id = request["items"][0]["id"]

        output = self.__download_file_to_df(service, report_id, file_id)

        self.__upload_with_default_azure_credential(
            account_url, container_name, file_path, file_name, output)

    def __download_file_to_df(self, service, report_id, file_id):

        report_file = service.files().get(
            reportId=report_id, fileId=file_id).execute()

        if report_file['status'] == 'REPORT_AVAILABLE':
            download_url = report_file["urls"]["apiUrl"]
            response, content = self.http.request(download_url)

            content_split = content.split(b"\n\nReport Fields\n")
            s = str(content_split[1], "utf-8")
            data = StringIO(s)
            df = pd.read_csv(data)
            df = df[:-1]

            output = df.to_csv(index=False)

        return output

    def __upload_with_default_azure_credential(self, account_url, container_name, file_path, file_name, data):
        credential = DefaultAzureCredential()
        blob_client = BlobClient(
            account_url, container_name, file_path + file_name, credential)
        blob_client.upload_blob(data)

    def __upload_with_sas(self, account_url, file_path, file_name, sas_token, data):
        sas_url = account_url + file_path + file_name + '?' + sas_token
        encoded_data = data.encode("utf-8")
        content_type_string = ContentSettings()
        r = rq.put(sas_url,
                   data=encoded_data,
                   headers={
                       'content-type': content_type_string.content_type,
                       'x-ms-blob-type': 'BlockBlob'
                   }
                   )
        return print(r.text)

    def __download_with_sas(self, account_url, file_path, file_name, sas_token):
        sas_url = account_url + file_path + file_name + '?' + sas_token
        with urlopen(sas_url) as response:
            id = response.read()
        return id.decode("utf-8")
