import logging
from typing import Generator, Union
from datetime import datetime, timedelta

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.core.error import *

from ..conf.cost_conf import BIGQUERY_TABLE_PREFIX
from ..connector.bigquery_connector import BigqueryConnector

_LOGGER = logging.getLogger('spaceone')

REQUIRED_TASK_OPTIONS = ["start", "billing_export_project_id", "billing_dataset_id", "billing_account_id"]
REQUIRED_OPTIONS = ["billing_export_project_id", "billing_dataset_id", "billing_account_id"]
EXCLUSIVE_PRODUCT = ['Invoice']

class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bigquery_connector = BigqueryConnector()
        self.billing_export_project_id = None
        self.billing_dataset = None
        self.billing_table = None

    def get_linked_accounts(self, options: dict, secret_data: dict, schema: str) -> dict:

        linked_accounts = []
        self.bigquery_connector.create_session(options, secret_data, schema)
        self._check_options(options)

        self.billing_export_project_id = options['billing_export_project_id']
        self.billing_dataset = options['billing_dataset_id']
        billing_account_id = options['billing_account_id']

        self.billing_table = f'{BIGQUERY_TABLE_PREFIX}_{billing_account_id.replace("-", "_")}'
        self._validate_table_exists()

        start_month = self._get_start_month()

        query = self._create_linked_accounts_google_sql(start_month)
        response_stream = self.bigquery_connector.read_df_from_bigquery(query)
        for index, row in response_stream.iterrows():
            _LOGGER.debug(f'[get_linked_accounts] row: {row}]')
            if row.id is not None:
                linked_accounts.append({
                        'account_id': row.id,
                        'name': row.project_name
                    })

        return {'results': linked_accounts}

    def get_data(
            self, options: dict, secret_data: dict, task_options: dict, schema: str = None
    ) -> Generator[dict, None, None]:
        self.bigquery_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options['start']
        self.billing_export_project_id = task_options['billing_export_project_id']
        self.billing_dataset = task_options['billing_dataset_id']
        billing_account_id = task_options['billing_account_id']
        self.target_project_id = task_options['project_id']

        self.billing_table = f'{BIGQUERY_TABLE_PREFIX}_{billing_account_id.replace("-", "_")}'
        self._validate_table_exists()

        _LOGGER.debug(f'[get_data] task_options: {task_options} / start: {start})')

        query = self._create_google_sql(start)
        response_stream = self.bigquery_connector.read_df_from_bigquery(query)
        for index, row in response_stream.iterrows():
            yield self._make_cost_data(row)

        yield {"results": []}

    def _make_cost_data(self, row) -> dict:
        """ Source Data Model (DataFrame)
        class CostSummaryItem(DataFrame):
            billed_at: str
            billing_account_id: str
            sku_description: str
            id: str
            name: str
            region_code: str
            currency_conversion_rate: float
            pricing_unit: str
            month: str
            cost_type: str
            labels: str(list of dict)
            cost: float
            usage_quantity: float
        """
        costs_data = []

        try:
            if row.product not in EXCLUSIVE_PRODUCT:
                data = {
                    'cost': row.cost,
                    'usage_quantity': row.usage_quantity,
                    'provider': 'google_cloud',
                    'product': row.description,
                    'region_code': row.region_code,
                    'usage_type': row.sku_description,
                    'usage_unit': row.pricing_unit,
                    'billed_date': self._change_datetime_to_string(row.billed_at),
                    'additional_info': {
                        'Project ID': row.id,
                        'Project Name': row.project_name,
                        'Billing Account ID': row.billing_account_id,
                        'Cost Type': row.cost_type,
                        'Invoice Month': row.month,
                    },
                    'tags': {}
                }

                if labels := eval(row.labels):
                    for label_object in labels:
                        data['tags'][label_object['key']] = label_object['value']

                costs_data.append(data)

        except Exception as e:
            _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
            raise e

        return {"results": costs_data}

    @staticmethod
    def _check_task_options(task_options):
        missing_keys = [key for key in REQUIRED_TASK_OPTIONS if key not in task_options]
        if missing_keys:
            for key in missing_keys:
                raise ERROR_REQUIRED_PARAMETER(key=f"task_options.{key}")

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
        if self.target_project_id != '*':
            where_condition += f" AND project.id = '{self.target_project_id}'"

        query = f"""
            SELECT
              timestamp_trunc(usage_start_time, DAY) as billed_at,
              billing_account_id,
              service.description,
              sku.description as sku_description,
              project.id,
              project.name as project_name,
              IFNULL((location.region), 'global') as region_code,
              usage.pricing_unit,
              invoice.month,
              cost_type,
              TO_JSON_STRING(labels) as labels,

              SUM(cost)
                + SUM(IFNULL((SELECT SUM(c.amount)
                              FROM UNNEST(credits) c), 0))
                AS cost,

              SUM(usage.amount_in_pricing_units) as usage_quantity,
            FROM `{self.billing_export_project_id}.{self.billing_dataset}.{self.billing_table}`
            {where_condition}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
            ORDER BY billed_at desc
            ;
        """
        return query

    def _create_linked_accounts_google_sql(self, start):
        where_condition = f"""
        WHERE usage_start_time >= TIMESTAMP('{start}-01')
        """

        query = f"""
            SELECT
            distinct project.id, project.name as project_name
            FROM `{self.billing_export_project_id}.{self.billing_dataset}.{self.billing_table}`
            {where_condition}
            ;
        """
        return query

    @staticmethod
    def _change_datetime_to_string(date_time):
        return str(date_time.strftime("%Y-%m-%d"))

    @staticmethod
    def _get_start_month():
        start_time: datetime = datetime.utcnow() - timedelta(days=365)
        start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")