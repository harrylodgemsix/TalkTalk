import json
import logging
import os
from datetime import date, timedelta

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlockBlobService, PublicAccess

from shared_code import dcm_class

# log into key vault and get key
KVUri = os.environ["KVUri"]
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

JSON_KEY = client.get_secret("google-service-account-json-file")
JSON_KEY_VALUE = eval(JSON_KEY.value)

BLOB_ACCESS_KEY_SECRET = client.get_secret("blob-account-key")
BLOB_ACCESS_KEY_VALUE = BLOB_ACCESS_KEY_SECRET.value

ACCOUNT_URL_SECRET = client.get_secret("blob-account-url")
ACCOUNT_URL = ACCOUNT_URL_SECRET.value

SAS_SECRET_NAME = os.environ["integration_sas_secret_all"]
SAS_TOKEN_SECRET = client.get_secret(SAS_SECRET_NAME)
SAS_TOKEN = SAS_TOKEN_SECRET.value

file_name = date.today() - timedelta(days=1)


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_json = req.get_json()
    account_id = req_json.get('account_id')
    report_id = req_json.get('report_id')
    upload_folder = req_json.get('upload_folder')

    upload_folder_path = "/new/" + str(upload_folder) + "/"

    dcm = dcm_class.dcmConnectorForAzure(account_id, JSON_KEY_VALUE)
    dcm.authenticate_using_service_account()
    dcm.download_report_upload_to_azure(
        report_id, ACCOUNT_URL, "bronze", upload_folder_path, file_name)

    return func.HttpResponse(json.dumps({"status": "success"}))
