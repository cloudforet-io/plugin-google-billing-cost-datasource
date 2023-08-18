LOG = {
    'filters': {
        'masking': {
            'rules': {
                'DataSource.verify': [
                    'secret_data'
                ],
                'Job.get_tasks': [
                    'secret_data'
                ],
                'Cost.get_data': [
                    'secret_data'
                ]
            }
        }
    }
}

DEFAULT_LOGGER = 'cloudforet'

CONNECTORS = {
    'SpaceConnector': {
        'backend': 'cloudforet.core.connector.space_connector.SpaceConnector',
        'endpoints': {
            'identity': 'grpc+ssl://identity.api.dev.cloudforet.dev:443/v1'
        }
    },
}

SECRET_TYPE_DEFAULT = 'USE_SERVICE_ACCOUNT_SECRET'
