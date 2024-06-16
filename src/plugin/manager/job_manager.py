import logging
from datetime import datetime, timedelta

from spaceone.core.error import *
from spaceone.core.manager import BaseManager

from ..conf.cost_conf import BIGQUERY_TABLE_PREFIX
from ..connector.bigquery_connector import BigqueryConnector


_LOGGER = logging.getLogger('spaceone')

REQUIRED_OPTIONS = ["billing_export_project_id", "billing_dataset_id", "billing_account_id"]


class JobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bigquery_connector = BigqueryConnector()
        self.billing_export_project_id = None
        self.billing_dataset = None
        self.billing_table = None

    def get_tasks(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None) -> dict:

        self.bigquery_connector.create_session(options, secret_data, schema)
        self._check_options(options)

        self.billing_export_project_id = options['billing_export_project_id']
        self.billing_dataset = options['billing_dataset_id']
        billing_account_id = options['billing_account_id']

        self.billing_table = f'{BIGQUERY_TABLE_PREFIX}_{billing_account_id.replace("-", "_")}'
        self._validate_table_exists()

        tasks = []
        changed = []

        start_month = self._get_start_month(start, last_synchronized_at)

        query = self._create_google_sql(start_month)
        response_stream = self.bigquery_connector.read_df_from_bigquery(query)

        for index, row in response_stream.iterrows():
            tasks.append(
                {
                    "task_options": {
                        "start": start_month,
                        "compartment_id": row.id
                    }
                }
            )

        changed.append({"start": start_month})

        return {"tasks": tasks, "changed": changed}

    def _get_start_month(self, start, last_synchronized_at=None):
        if start:
            start_time: datetime = self._parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")

    @staticmethod
    def _parse_start_time(start_str):
        date_format = "%Y-%m"

        try:
            return datetime.strptime(start_str, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)

    @staticmethod
    def _check_options(options):
        missing_keys = [key for key in REQUIRED_OPTIONS if key not in options]
        if missing_keys:
            for key in missing_keys:
                raise ERROR_REQUIRED_PARAMETER(key=f"options.{key}")

    def _validate_table_exists(self):
        bigquery_tables_info = self.bigquery_connector.list_tables(self.billing_export_project_id, self.billing_dataset)
        bigquery_table_names = [table_info["tableReference"]["tableId"] for table_info in bigquery_tables_info]

        if self.billing_table not in bigquery_table_names:
            raise ERROR_REQUIRED_PARAMETER(key=f"not found table {bigquery_table_names}")

    def _create_google_sql(self, start):
        where_condition = f"""
        WHERE usage_start_time >= TIMESTAMP('{start}-01')
        """

        query = f"""
            SELECT
            distinct project.id
            FROM `{self.billing_export_project_id}.{self.billing_dataset}.{self.billing_table}`
            {where_condition}
            ;
        """
        return query
