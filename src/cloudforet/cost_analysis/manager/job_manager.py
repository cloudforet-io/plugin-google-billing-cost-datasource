import logging
import re
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.error import *
from cloudforet.cost_analysis.conf.cost_conf import *
from cloudforet.cost_analysis.connector import BigqueryConnector, CloudBillingConnector
from cloudforet.cost_analysis.model import Tasks

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bigquery_connector: BigqueryConnector = self.locator.get_connector(BigqueryConnector)
        self.cloud_billing_connector: CloudBillingConnector = self.locator.get_connector(CloudBillingConnector)

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):

        tasks = []
        changed = []

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time

        self.bigquery_connector.create_session(options, secret_data, schema)
        self.cloud_billing_connector.create_session(options, secret_data, schema)

        billing_dataset = self._get_billing_dataset_from_secret_data(secret_data)

        billing_info = self.cloud_billing_connector.get_billing_info()
        prefix, sub_billing_account = billing_info['billingAccountName'].split('/')

        secret_type = options.get('secret_type', SECRET_TYPE_DEFAULT)

        if secret_type == 'MANUAL':
            # NOT IMPLEMENTED
            pass
        elif secret_type == 'USE_SERVICE_ACCOUNT_SECRET':
            task = {
                'task_options': {
                    'start': start_date,
                    'billing_dataset': billing_dataset,
                    'sub_billing_account': sub_billing_account,
                    'project_id': secret_data['project_id']
                }
            }

            tasks.append(task)
            changed.append({'start': changed_time})
        else:
            raise ERROR_INVALID_SECRET_TYPE(secret_type=options.get('secret_type'))

        tasks = Tasks({'tasks': tasks, 'changed': changed})
        tasks.validate()
        return tasks.to_primitive()

    @staticmethod
    def _get_start_time(start, last_synchronized_at=None):
        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return start_time

    @staticmethod
    def _get_billing_dataset_from_secret_data(secret_data):
        if not secret_data.get('billing_dataset'):
            _LOGGER.info(
                f'[get_tasks] Not exist billing_dataset in secret_data. Use default: {DEFAULT_BILLING_DATASET}')
            billing_dataset = DEFAULT_BILLING_DATASET
        else:
            _LOGGER.info(f'[get_tasks] Use billing_dataset in secret_data: {secret_data["billing_dataset"]}')
            billing_dataset = secret_data['billing_dataset']
        return billing_dataset
