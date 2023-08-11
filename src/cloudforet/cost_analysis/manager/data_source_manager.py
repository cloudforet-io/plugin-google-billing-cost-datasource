import logging

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector import BigQueryConnector
from cloudforet.cost_analysis.model import PluginMetadata

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options):
        plugin_metadata = PluginMetadata()
        plugin_metadata.validate()

        return {
            'metadata': plugin_metadata.to_primitive()
        }

    def verify_plugin(self, options, secret_data, schema):
        big_query_connector: BigQueryConnector = self.locator.get_connector(BigQueryConnector)
        big_query_connector.create_session(options, secret_data, schema)
