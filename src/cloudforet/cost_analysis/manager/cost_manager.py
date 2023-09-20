import logging

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.error import *
from cloudforet.cost_analysis.conf.cost_conf import BIGQUERY_TABLE_PREFIX
from cloudforet.cost_analysis.connector import BigqueryConnector

_LOGGER = logging.getLogger(__name__)

REQUIRED_TASK_OPTIONS = ["start", "billing_dataset", "billing_account_id", "project_id"]
EXCLUSIVE_PRODUCT = ['Invoice']


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
        billing_account_id = task_options['billing_account_id']
        self.target_project_id = task_options['project_id']

        self.billing_table = f'{BIGQUERY_TABLE_PREFIX}_{billing_account_id.replace("-", "_")}'
        self._validate_table_exists()

        _LOGGER.debug(f'[get_data] task_options: {task_options} / start: {start})')

        query = self._create_google_sql(start)
        response_stream = self.bigquery_connector.read_df_from_bigquery(query)
        for index, row in response_stream.iterrows():
            yield self._make_cost_data(row)

        yield []

    @staticmethod
    def _check_task_options(task_options):
        missing_keys = [key for key in REQUIRED_TASK_OPTIONS if key not in task_options]
        if missing_keys:
            for key in missing_keys:
                raise ERROR_REQUIRED_PARAMETER(key=f"task_options.{key}")

    def _validate_table_exists(self):
        bigquery_tables_info = self.bigquery_connector.list_tables(self.billing_dataset)
        bigquery_table_names = [table_info["tableReference"]["tableId"] for table_info in bigquery_tables_info]

        if self.billing_table not in bigquery_table_names:
            raise ERROR_NOT_FOUND_TABLE(table=self.billing_table, dataset=self.billing_dataset)

    def _make_cost_data(self, row):
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
                    'cost': row.cost_at_list,
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
                        'Charged Cost': row.cost
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

        return costs_data

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
              currency_conversion_rate,
              usage.pricing_unit,
              invoice.month,
              cost_type,
              TO_JSON_STRING(labels) as labels,
            
              SUM(cost)
                + SUM(IFNULL((SELECT SUM(c.amount)
                              FROM UNNEST(credits) c), 0))
                AS cost,
            
              SUM(usage.amount_in_pricing_units) as usage_quantity,
            FROM `{self.billing_project_id}.{self.billing_dataset}.{self.billing_table}`
            {where_condition}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            ORDER BY billed_at desc
            ;
        """
        return query

    @staticmethod
    def _change_datetime_to_string(date_time):
        return str(date_time.strftime("%Y-%m-%d"))
