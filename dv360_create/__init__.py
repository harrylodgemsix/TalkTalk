import datetime
import json
import logging
import os
from datetime import date, timedelta

import azure.functions as func
import httplib2
import pandas as pd
import requests as rq
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlockBlobService, ContentSettings, PublicAccess
from googleapiclient import discovery
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from oauth2client.service_account import ServiceAccountCredentials

from shared_code import dv360_class
from .report_definiton import report_definition_generator

KVUri = os.environ["KVUri"]
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

JSON_KEY = client.get_secret("google-service-account-json-file")
JSON_KEY_VALUE = eval(JSON_KEY.value)

ACCOUNT_URL_SECRET = client.get_secret("blob-account-url")
ACCOUNT_URL = ACCOUNT_URL_SECRET.value

SAS_SECRET_NAME = os.environ["integration_sas_secret_all"]
SAS_TOKEN_SECRET = client.get_secret(SAS_SECRET_NAME)
SAS_TOKEN = SAS_TOKEN_SECRET.value


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_json = req.get_json()
    advertiser_id_list = req_json.get("advertiser_id")
    file_path = req_json.get("file_path")

    dv360 = dv360_class.dv360ConnectorForAzure()
    dv360.authenticate(JSON_KEY_VALUE)
    dv360.create_report(
        report_definition_generator(advertiser_id_list))
    return func.HttpResponse(json.dumps({"report_id": int(dv360.report_id), "file_path": str(file_path)}))
