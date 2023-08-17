import logging
import io
from datetime import datetime, timedelta
from dateutil import rrule

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.error import *
from cloudforet.cost_analysis.conf.cost_conf import BIGQUERY_TABLE_PREFIX
from cloudforet.cost_analysis.connector import BigqueryConnector

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bigquery_connector: BigqueryConnector = self.locator.get_connector(BigqueryConnector)
        self.billing_project_id = None
        self.billing_dataset = None
        self.billing_table = None
        self.target_project_id = None

    def get_data(self, options, secret_data, schema, task_options):
        self.bigquery_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options['start']
        self.billing_project_id = secret_data['project_id']
        self.billing_dataset = task_options['billing_dataset']
        sub_billing_account = task_options['sub_billing_account']
        self.target_project_id = task_options['target_project_id']

        self.billing_table = f'{BIGQUERY_TABLE_PREFIX}_{sub_billing_account.replace("-", "_")}'
        bigquery_tables_info = self.bigquery_connector.list_tables(self.billing_dataset)
        bigquery_table_names = [table_info['tableReference']['tableId'] for table_info in bigquery_tables_info]

        if self.billing_table not in bigquery_table_names:
            raise ERROR_NOT_FOUND_TABLE(table=self.billing_table, dataset=self.billing_dataset)

        _LOGGER.debug(f'[get_data] task_options: {task_options} / start: {start})')

        query = self._create_google_sql(start)
        response_stream = self.bigquery_connector.read_df_from_bigquery(query)
        for index, row in response_stream.iterrows():
            yield self._make_cost_data(row)

        yield []

    @staticmethod
    def _check_task_options(task_options):
        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if 'billing_dataset' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.billing_dataset')

        if 'sub_billing_account' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.sub_billing_account')

        if 'target_project_id' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.target_project_id')

    @staticmethod
    def _get_date_range(start):
        date_ranges = []
        start_time = datetime.strptime(start, '%Y-%m-%d')
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            billed_month = dt.strftime('%Y-%m')
            date_ranges.append(billed_month)

        return date_ranges

    @staticmethod
    def _make_cost_data(row):
        costs_data = []
        try:
            data = {
                'cost': row.cost * (1 / row.currency_conversion_rate),
                'currency': 'USD',
                'usage_quantity': row.usage_quantity,
                'provider': 'google_cloud',
                'product': row.description,
                'region_code': row.region_code,
                'account': row.id,
                'usage_type': row.sku_description,
                'usage_unit': row.pricing_unit,
                'billed_at': row.billed_at,
                'additional_info': {
                    'Project Name': row.name,
                    'Billing Account ID': row.billing_account_id,
                    'Cost Type': row.cost_type,
                    'Invoice Month': row.month
                },
            }

        except Exception as e:
            _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
            raise e

        costs_data.append(data)

        return costs_data

    def _create_google_sql(self, start):
        where_condition = f"""
        WHERE usage_start_time >= TIMESTAMP('{start}')
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
              project.name,
              IFNULL((location.region), 'global') as region_code,
              currency_conversion_rate,
              usage.pricing_unit,
              invoice.month,
              cost_type,
            
              SUM(cost)
                + SUM(IFNULL((SELECT SUM(c.amount)
                              FROM UNNEST(credits) c), 0))
                AS cost,
            
              SUM(usage.amount_in_pricing_units) as usage_quantity,
            FROM `{self.billing_project_id}.{self.billing_dataset}.{self.billing_table}`
            {where_condition}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
            ORDER BY billed_at desc
            ;
        """
        return query
