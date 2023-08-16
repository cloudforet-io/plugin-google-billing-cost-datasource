import logging
import google.oauth2.service_account
from googleapiclient.discovery import build

from spaceone.core.connector import BaseConnector
from cloudforet.cost_analysis.error import *

_LOGGER = logging.getLogger(__name__)


class CloudBillingConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = None
        self.google_client = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)
        self.project_id = secret_data['project_id']

        credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
        self.google_client = build('cloudbilling', 'v1', credentials=credentials)

    def get_billing_info(self):
        billing_info = self.google_client.projects().getBillingInfo(name=f'projects/{self.project_id}').execute()
        return billing_info

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
