import logging

from spaceone.core.manager import BaseManager
from ..connector.bigquery_connector import BigqueryConnector

_LOGGER = logging.getLogger('spaceone')


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options: dict) -> dict:
        metadata = {
            "currency": "KRW",
            "supported_secret_types": ["MANUAL"],
            "use_account_routing": False,
            "data_source_rules": [
                {
                    "name": "match_service_account",
                    "conditions_policy": "ALWAYS",
                    "actions": {
                        "match_service_account": {
                            "source": "additional_info.Project ID",
                            "target": "data.project_id",
                        }
                    },
                    "options": {"stop_processing": True},
                }
            ],
        }
        if options.get("use_account_routing", False):
            metadata["use_account_routing"] = True
            if account_match_key := options.get("account_match_key", "additional_info.Project ID"):
                metadata["account_match_key"] = account_match_key

        return {"metadata": metadata}

    @staticmethod
    def verify_plugin(
        options: dict, secret_data: dict, domain_id: str, schema: str = None
    ) -> None:

        bigquery_connector = BigqueryConnector()
        bigquery_connector.create_session(options, secret_data, schema)
