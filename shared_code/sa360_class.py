from io import BytesIO

import httplib2
import pandas as pd
from apiclient.discovery import build
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import (BlobClient, BlockBlobService, ContentSettings,
                                PublicAccess)
from google.oauth2 import service_account
from oauth2client.client import OAuth2Credentials


class sa360ConnectorForAzure():
    def __init__(self) -> None:
        self.service = None
        self.report_id = None
        self.data_to_upload = None

    def authenticate(self, json_key_file):
        # authenticates with service account key file
        # creates a sa360 service variable
        credentials = service_account.Credentials.from_service_account_info(
            json_key_file)
        service = build('doubleclicksearch', 'v2', credentials=credentials)
        self.service = service

        return

    def create_report(self, agencyId, advertiserId, start_date, end_date):
        # creates a report request for ads report
        request = self.service.reports().request(body={
            "reportType": "ad",
            "columns": [
                {'columnName': 'status'},
                {'columnName': 'engineStatus'},
                {'columnName': 'creationTimestamp'},
                {'columnName': 'lastModifiedTimestamp'},
                {'columnName': 'agency'},
                {'columnName': 'agencyId'},
                {'columnName': 'advertiser'},
                {'columnName': 'advertiserId'},
                {'columnName': 'account'},
                {'columnName': 'accountId'},
                {'columnName': 'accountEngineId'},
                {'columnName': 'accountType'},
                {'columnName': 'campaign'},
                {'columnName': 'campaignId'},
                {'columnName': 'campaignStatus'},
                {'columnName': 'adGroup'},
                {'columnName': 'adGroupId'},
                {'columnName': 'adGroupStatus'},
                {'columnName': 'ad'},
                {'columnName': 'adId'},
                {'columnName': 'adEngineId'},
                {'columnName': 'isUnattributedAd'},
                {'columnName': 'adHeadline'},
                {'columnName': 'adHeadline2'},
                {'columnName': 'adHeadline3'},
                {'columnName': 'adDescription1'},
                {'columnName': 'adDescription2'},
                {'columnName': 'adDisplayUrl'},
                {'columnName': 'adLandingPage'},
                {'columnName': 'adType'},
                {'columnName': 'adPromotionLine'},
                {'columnName': 'adLabels'},
                {'columnName': 'adPathField1'},
                {'columnName': 'adPathField2'},
                {'columnName': 'effectiveLabels'},
                {'columnName': 'dfaActions'},
                {'columnName': 'dfaRevenue'},
                {'columnName': 'dfaTransactions'},
                {'columnName': 'dfaWeightedActions'},
                {'columnName': 'dfaActionsCrossEnv'},
                {'columnName': 'dfaRevenueCrossEnv'},
                {'columnName': 'dfaTransactionsCrossEnv'},
                {'columnName': 'dfaWeightedActionsCrossEnv'},
                {'columnName': 'avgCpc'},
                {'columnName': 'avgCpm'},
                {'columnName': 'avgPos'},
                {'columnName': 'clicks'},
                {'columnName': 'cost'},
                {'columnName': 'ctr'},
                {'columnName': 'impr'},
                {'columnName': 'adWordsConversions'},
                {'columnName': 'adWordsConversionValue'},
                {'columnName': 'adWordsViewThroughConversions'},
                {'columnName': 'visits'},
                {'columnName': 'date'},
                {'columnName': 'deviceSegment'}
            ],
            "reportScope": {
                "agencyId": agencyId,
                "advertiserId": advertiserId
            },
            "statisticsCurrency": "agency",
            "maxRowsPerFile": 100000000,
            "downloadFormat": "CSV",
            "timeRange": {
                "startDate": start_date,
                "endDate": end_date
            }
        }
        )

        json_data = request.execute()
        self.report_id = json_data["id"]
        return self.report_id

    def get_report(self, report_id):
        # checks if report is ready to be downloaded
        # converts bytes data to csv data ready for upload
        if report_id:
            self.report_id = report_id

        request = self.service.reports().get(reportId=report_id)
        json_obj = request.execute()

        if json_obj["isReportReady"]:
            download_request = self.service.reports().getFile(
                reportId=report_id, reportFragment=0)

            bytes_data = download_request.execute()
            data = BytesIO(bytes_data)
            self.df = pd.read_csv(data)

            self.data_to_upload = self.df.to_csv(index=False)

        return self.data_to_upload

    def upload_with_default_azure_credential(
            self,
            account_url,
            container_name,
            file_path,
            file_name):
        # uploads the data from the get_report method to an azure blob container
        credential = DefaultAzureCredential()
        blob_client = BlobClient(
            account_url, container_name, file_path + file_name, credential)
        blob_client.upload_blob(self.data_to_upload)
