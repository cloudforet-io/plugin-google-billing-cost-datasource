from spaceone.core.error import *


class ERROR_TOO_MANY_CSV_FILES(ERROR_UNKNOWN):
    _message = 'Too many csv files: {target_dir}'


class ERROR_EXCHANGE_RATE_DATA_NOT_FOUND(ERROR_UNKNOWN):
    _message = 'Exchange rate data not found'


class ERROR_NOT_FOUND_EXCHANGE_RATE(ERROR_UNKNOWN):
    _message = 'Invalid exchange rate: {year}-{month}'


class ERROR_NOT_FOUND_BUCKET(ERROR_UNKNOWN):
    _message = 'Invalid bucket: {bucket}'


class ERROR_NOT_VALID_ORGANIZATION(ERROR_UNKNOWN):
    _message = 'Not valid organization: {organization}'
