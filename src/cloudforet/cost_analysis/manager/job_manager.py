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
        self.billing_account_id = None

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):

        tasks = []
        changed = []

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time

        self.bigquery_connector.create_session(options, secret_data, schema)
        self.cloud_billing_connector.create_session(options, secret_data, schema)
        self._check_target_project_id_in_secret_data(secret_data)

        billing_dataset = self._get_billing_dataset_from_secret_data(secret_data)

        if secret_data.get('target_billing_account_id'):
            self.billing_account_id = secret_data['target_billing_account_id']
        else:
            billing_info = self.cloud_billing_connector.get_billing_info()
            prefix, self.billing_account_id = billing_info['billingAccountName'].split('/')

        target_project_ids = self._get_target_project_ids(secret_data['target_project_id'])

        secret_type = options.get('secret_type', SECRET_TYPE_DEFAULT)

        if secret_type == 'MANUAL':
            # NOT IMPLEMENTED
            pass
        elif secret_type == 'USE_SERVICE_ACCOUNT_SECRET':

            if target_project_ids:
                for target_project_id in target_project_ids:
                    task, change_info = self._generate_task_and_change_info(start_date, changed_time, billing_dataset,
                                                                            target_project_id)
                    tasks.append(task)
                    changed.append(change_info)
            else:
                target_project_id = '*'
                task, change_info = self._generate_task_and_change_info(start_date, changed_time, billing_dataset,
                                                                        target_project_id)
                tasks.append(task)
                changed.append(change_info)
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
    def _check_target_project_id_in_secret_data(secret_data):
        if not secret_data.get('target_project_id'):
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.target_project_id')

    @staticmethod
    def _get_billing_dataset_from_secret_data(secret_data):
        if not secret_data.get('billing_dataset'):
            _LOGGER.debug(
                f'[get_tasks] Not exist billing_dataset in secret_data. Use default: {DEFAULT_BILLING_DATASET}')
            billing_dataset = DEFAULT_BILLING_DATASET
        else:
            _LOGGER.debug(f'[get_tasks] Use billing_dataset in secret_data: {secret_data["billing_dataset"]}')
            billing_dataset = secret_data['billing_dataset']
        return billing_dataset

    def _get_target_project_ids(self, target_project_id: list):
        if not target_project_id:
            _LOGGER.info(f'[get_tasks] Not exist target_project_id: {self.billing_account_id}')
            raise ERROR_NOT_EXIST_TARGET_PROJECT_ID(target_project_id=target_project_id)

        elif '*' in target_project_id:
            _LOGGER.info(f'[get_tasks] Use all projects in billing account: {self.billing_account_id}')
            return []
        else:
            return target_project_id

    def _generate_task_and_change_info(self, start_date, changed_time, billing_dataset, target_project_id):
        task = {
            'task_options': {
                'start': start_date,
                'billing_dataset': billing_dataset,
                'billing_account_id': self.billing_account_id,
                'target_project_id': target_project_id
            }
        }

        changed = {'start': changed_time}

        return task, changed
