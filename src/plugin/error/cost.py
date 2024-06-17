from spaceone.core.error import *


class ERROR_INVALID_SECRET_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'Invalid secret type: {secret_type}'


class ERROR_TOO_MANY_CSV_FILES(ERROR_UNKNOWN):
    _message = 'Too many csv files: {target_dir}'


class ERROR_EXCHANGE_RATE_DATA_NOT_FOUND(ERROR_UNKNOWN):
    _message = 'Exchange rate data not found'


class ERROR_NOT_FOUND_EXCHANGE_RATE(ERROR_UNKNOWN):
    _message = 'Invalid exchange rate: {year}-{month}'


class ERROR_NOT_FOUND_TABLE(ERROR_UNKNOWN):
    _message = 'Not found table: {table} / dataset: {dataset}'


class ERROR_NOT_EXIST_TARGET_PROJECT_ID(ERROR_INVALID_ARGUMENT):
    _message = 'Not exist target_project_id: {target_project_id}'
