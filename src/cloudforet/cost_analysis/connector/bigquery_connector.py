import logging
import google.oauth2.service_account
from googleapiclient.discovery import build

from spaceone.core.connector import BaseConnector
from cloudforet.cost_analysis.error import *

MAX_OBJECTS = 100000

_LOGGER = logging.getLogger(__name__)


class BigqueryConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = None
        self.google_client = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)
        self.project_id = secret_data['project_id']

        credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
        self.google_client = build('bigquery', 'v2', credentials=credentials)

    def list_dataset(self, **query):
        dataset_list = []
        query.update({'projectId': self.project_id, 'all': True})
        request = self.google_client.datasets().list(**query)
        while request is not None:
            response = request.execute()
            for dataset in response.get('datasets', []):
                dataset_list.append(dataset)
            request = self.google_client.datasets().list_next(previous_request=request, previous_response=response)

        return dataset_list

    def get_dataset(self, dataset_id, **query):
        query.update({'projectId': self.project_id,
                      'datasetId': dataset_id})
        response = {}
        response = self.google_client.datasets().get(**query).execute()

        return response

    def list_job(self, **query):
        job_list = []
        query.update({'projectId': self.project_id,
                      'allUsers': True,
                      'projection': 'full'})
        request = self.google_client.jobs().list(**query)
        while request is not None:
            response = request.execute()
            for job in response.get('jobs', []):
                job_list.append(job)
            request = self.google_client.jobs().list_next(previous_request=request, previous_response=response)

        return job_list

    def list_projects(self, **query):
        project_list = []
        request = self.google_client.projects().list(**query)

        while request is not None:
            response = request.execute()
            for project in response.get('projects', []):
                project_list.append(project)
            request = self.google_client.projects().list_next(previous_request=request, previous_response=response)

        return project_list

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

    def get_tables(self, dataset_id, table_id, **query):
        query.update({'projectId': self.project_id,
                      'datasetId': dataset_id,
                      'tableId': table_id})
        response = self.google_client.tables().get(**query).execute()

        return response

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
