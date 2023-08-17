import logging
import google.oauth2.service_account
import pandas_gbq
from googleapiclient.discovery import build

from spaceone.core.connector import BaseConnector
from cloudforet.cost_analysis.error import *

_LOGGER = logging.getLogger(__name__)


class BigqueryConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = None
        self.credentials = None
        self.google_client = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)
        self.project_id = secret_data['project_id']

        self.credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
        self.google_client = build('bigquery', 'v2', credentials=self.credentials)

    def list_tables(self, dataset_id, **query):
        table_list = []

        query.update({'projectId': self.project_id,
                      'datasetId': dataset_id})

        request = self.google_client.tables().list(**query)
        while request is not None:
            response = request.execute()
            for table in response.get('tables', []):
                table_list.append(table)
            request = self.google_client.tables().list_next(previous_request=request, previous_response=response)

        return table_list

    def read_df_from_bigquery(self, query):
        return pandas_gbq.read_gbq(query, project_id=self.project_id, credentials=self.credentials)

    @staticmethod
    def _check_secret_data(secret_data):
        if 'project_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.project_id')

        if 'private_key' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.private_key')

        if 'token_uri' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.token_uri')

        if 'client_email' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_email')
