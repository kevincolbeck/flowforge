"""
CRM Connectors

Connect to CRM platforms for customer data operations.
"""

from .salesforce import SalesforceConnector
from .hubspot import HubSpotConnector
from .zoho import ZohoCRMConnector
from .pipedrive import PipedriveConnector
from .freshsales import FreshsalesConnector

# Alias for registry compatibility
ZohoConnector = ZohoCRMConnector

__all__ = [
    "SalesforceConnector",
    "HubSpotConnector",
    "ZohoCRMConnector",
    "ZohoConnector",
    "PipedriveConnector",
    "FreshsalesConnector",
]
