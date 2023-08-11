import logging
import io
from datetime import datetime, timedelta
from dateutil import rrule

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.error import *
from cloudforet.cost_analysis.connector import GoogleStorageConnector

_LOGGER = logging.getLogger(__name__)

_PAGE_SIZE = 2000


class CostManager(BaseManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.google_storage_connector: GoogleStorageConnector = self.locator.get_connector(GoogleStorageConnector)
        self.bucket = None

    def get_data(self, options, secret_data, schema, task_options):
        self.google_storage_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)
        self.bucket = task_options['bucket']

        start = task_options['start']
        organization = task_options['organization']
        sub_billing_account = task_options['sub_billing_account']

        exchange_rate_data = self._get_exchange_rate_data()

        folders_info = self.google_storage_connector.list_objects(self.bucket)
        folder_names = [folder_info['name'] for folder_info in folders_info]

        exist_cost_data = False
        date_ranges = self._get_date_range(start)
        _LOGGER.debug(f'[get_data] task_options: {task_options} / date ranges: {date_ranges[0]} ~ {date_ranges[-1]})')

        for date in date_ranges:
            year, month = date.split('-')
            end_date = self._get_end_date(year, month)

            folder_path = f'{organization}/{sub_billing_account}/{year}/{month}/'
            if csv_file := self._get_csv_file_path(folder_path, folder_names):
                exist_cost_data = True

                blob = self.google_storage_connector.get_blob(self.bucket, csv_file)
                response_stream = self._get_cost_data(data=blob.download_as_bytes(),
                                                      target_file=csv_file)
                krw = self._set_exchange_rate(exchange_rate_data, year, month)

                for results in response_stream:
                    yield self._make_cost_data(results, end_date, krw)

            yield []

        if not exist_cost_data:
            _LOGGER.debug(
                f'[get_data] There is no cost data in {self.bucket}/{organization}/{sub_billing_account} folder.'
            )

    @staticmethod
    def _check_task_options(task_options):
        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if 'bucket' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.bucket')

        if 'organization' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.organization')

        if 'sub_billing_account' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.sub_billing_account')

    def _get_exchange_rate_data(self):
        exchange_path = 'settings/exchange_rate.csv'
        try:
            blob = self.google_storage_connector.get_blob(self.bucket, exchange_path)
            data_frame = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
            data_frame = data_frame.replace({np.nan: None})
            data_frame['year'].astype(int)
            data_frame['month'].astype(int)
            exchange_rate_data = data_frame.to_dict('records')
        except Exception as e:
            _LOGGER.error(f'[_get_exchange_rate_data] {e}')
            raise ERROR_EXCHANGE_RATE_DATA_NOT_FOUND()

        return exchange_rate_data

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
    def _get_end_date(year, month):
        next_month = datetime(int(year), int(month) + 1, 1) if month != '12' else datetime(int(year) + 1, 1, 1)
        end_date = next_month - timedelta(days=1)
        return end_date.strftime('%Y-%m-%d')

    @staticmethod
    def _set_exchange_rate(exchange_rate_data, year, month):
        krw = 0
        for exchange_rate in exchange_rate_data:
            if exchange_rate['year'] == int(year) and exchange_rate['month'] == int(month):
                krw = int(exchange_rate['KRW'])
                break
        if not krw:
            raise ERROR_NOT_FOUND_EXCHANGE_RATE(year=year, month=month)
        return krw

    def _get_csv_file_path(self, folder_path, folder_names):
        csv_files = [
            file_name for file_name in folder_names
            if file_name.startswith(folder_path) and file_name.endswith('.csv')
        ]

        if len(csv_files) > 1:
            raise ERROR_TOO_MANY_CSV_FILES(target_dir=csv_files)
        elif not csv_files:
            _LOGGER.debug(f'[get_csv_file_path] csv file not found (path: {self.bucket}/{folder_path})')
            return None
        else:
            return csv_files[0]

    def _get_cost_data(self, data, target_file):
        data_frame = pd.read_csv(io.BytesIO(data))
        data_frame = data_frame.replace({np.nan: None})

        data_frame = self._apply_strip_to_columns(data_frame)
        costs_data = data_frame.to_dict('records')

        _LOGGER.debug(f'[get_cost_data] costs count({target_file}): {len(costs_data)}')

        # Paginate
        page_count = int(len(costs_data) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield costs_data[offset:offset + _PAGE_SIZE]

    @staticmethod
    def _apply_strip_to_columns(data_frame):
        columns = list(data_frame.columns)
        columns = [column.strip() for column in columns]
        data_frame.columns = columns

        if isinstance(data_frame['소계'].values[0], str):
            data_frame['소계'] = data_frame['소계'].str.replace(',', '')
            data_frame['소계'] = data_frame['소계'].astype(float)

        return data_frame

    @staticmethod
    def _make_cost_data(results, end_date, krw):
        costs_data = []

        for result in results:
            try:
                data = {
                    'cost': result['소계'] * (1 / krw),
                    'currency': 'USD',
                    # 'usage_quantity': result['Usage'],
                    'provider': 'google_cloud',
                    'product': result['Service Name'],
                    # 'region_code': result.get('Region') if result.get('Region') else 'global',
                    'account': result.get('Project ID'),
                    'usage_type': result['SKU Name'],
                    # 'usage_unit': result['Usage Unit'],
                    'billed_at': datetime.strptime(end_date, '%Y-%m-%d'),
                    'additional_info': {
                        'Project Name': result.get('Project Name'),
                        'Sub Billing Account Name': result.get('SBA Name')
                        # 'Cost Type': result.get('Cost Type')
                    },
                }

            except Exception as e:
                _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
                raise e

            costs_data.append(data)

        return costs_data
