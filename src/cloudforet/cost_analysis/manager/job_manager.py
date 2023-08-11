import logging
import re
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.error import *
from cloudforet.cost_analysis.connector import GoogleStorageConnector
from cloudforet.cost_analysis.model import Tasks

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_storage_connector: GoogleStorageConnector = self.locator.get_connector(GoogleStorageConnector)

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):

        tasks = []
        changed = []

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time
        self.google_storage_connector.create_session(options, secret_data, schema)

        bucket = secret_data['bucket']
        organization = secret_data['organization']

        for gcs_bucket in self.google_storage_connector.list_buckets():
            if gcs_bucket['name'] == bucket:
                folders_info = self.google_storage_connector.list_objects(bucket)
                folder_paths = [folder_info['name'] for folder_info in folders_info]

                task_info = self._create_task_info(folder_paths)
                task_info = self._change_valid_task_info(task_info, organization, bucket)

                for organization, sub_billing_accounts in task_info.items():
                    for sub_billing_account in sub_billing_accounts:
                        tasks.append({
                            'task_options': {
                                'bucket': bucket,
                                'organization': organization,
                                'sub_billing_account': sub_billing_account,
                                'start': start_date
                            }
                        })
                        changed.append({'start': changed_time})

        tasks = Tasks({'tasks': tasks, 'changed': changed})
        tasks.validate()
        _LOGGER.debug(f'[get_tasks] create JobTasks: {tasks.to_primitive()}')
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
    def _create_task_info(folder_paths):
        task_info = {}
        for folder_path in folder_paths:
            try:
                organization, path = folder_path.split('/', 1)
                sub_billing_account_id, path = path.split('/', 1)
                if not task_info.get(organization):
                    task_info[organization] = [sub_billing_account_id]
                else:
                    if sub_billing_account_id not in task_info[organization]:
                        task_info[organization].append(sub_billing_account_id)
            except ValueError:
                continue
        return task_info

    def _change_valid_task_info(self, task_info, organization, bucket_name):
        valid_task_info = {}

        if organization == '*':
            for org_in_task_info in task_info:
                valid_task_info[org_in_task_info] = self._check_sub_billing_account_id(task_info[org_in_task_info],
                                                                                       org_in_task_info,
                                                                                       bucket_name)
        else:
            if organization in task_info:
                valid_task_info[organization] = self._check_sub_billing_account_id(task_info[organization],
                                                                                   organization,
                                                                                   bucket_name)
            else:
                _LOGGER.debug(f'[get_tasks] Not valid organization: {bucket_name}/{organization}')
                raise ERROR_NOT_VALID_ORGANIZATION(organization=organization)
        return valid_task_info

    @staticmethod
    def _check_sub_billing_account_id(sub_billing_account_ids, organization, bucket_name):
        pattern = r'^[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}'
        for sub_billing_account_id in sub_billing_account_ids:
            if not re.fullmatch(pattern, sub_billing_account_id):
                _LOGGER.debug(
                    f'[get_tasks] Not valid sub_billing_account_id: '
                    f'{bucket_name}/{organization}/{sub_billing_account_id}'
                )
                sub_billing_account_ids.remove(sub_billing_account_id)

        return sub_billing_account_ids
