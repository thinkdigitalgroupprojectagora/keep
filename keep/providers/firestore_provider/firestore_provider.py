"""
Firestore provider.
"""
import dataclasses
import os
import re
from typing import Optional

from firebase_admin import firestore, initialize_app, credentials

from pydantic.dataclasses import dataclass

from keep.providers.base.base_provider import BaseProvider
from keep.providers.models.provider_config import ProviderConfig


@dataclasses.dataclass
class FirestoreProviderAuthConfig:
    """
    Firestore authentication configuration.
    """

    credentials_path: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Path to the service account key in JSON format. "
            "If not provided, will use application default credentials",
        },
    )
    project_id: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "required": False,
            "description": "Google Cloud project ID. If not provided, "
            "it will try to fetch it from the environment variable 'GOOGLE_CLOUD_PROJECT'",
        },
    )

class FirestoreProvider(BaseProvider):
    provider_id: str
    config: ProviderConfig

    def __init__(self, provider_id: str, config: ProviderConfig):
        super().__init__(provider_id, config)

    def validate_config(self):
        """
        Validates required configuration for BigQuery provider.

        """
        if self.config.authentication is None:
            self.config.authentication = {}
        self.authentication_config = FirestoreProviderAuthConfig(
            **self.config.authentication
        )

        # Check for project_id and handle it here.
        if "project_id" not in self.config.authentication:
            try:
                self.config.authentication["project_id"] = os.environ[
                    "GOOGLE_CLOUD_PROJECT"
                ]
            except KeyError:
                raise ValueError(
                    "GOOGLE_CLOUD_PROJECT environment variable is not set."
                )
            if (
                self.config.authentication["project_id"] is None
                or self.config.authentication["project_id"] == ""
            ):
                # If default project not found, raise error
                raise ValueError("Firestore project id is missing.")

    def init_client(self):
        cred = credentials.ApplicationDefault()
        
        # Use a service account
        firestore_app = initialize_app(cred,{
            'projectId': self.config.authentication['project_id']
        })
        
        self.client = firestore.client(app=firestore_app)

    def dispose(self):
        pass # Nothing to dispose

    def notify(self, **kwargs):
        pass  # Define how to notify about any alerts or issues

    def _query(self, query: str):
        self.init_client()

        collection_and_document = re.search('FROM (.*)FETCH ', query).group(1).split('.')
        collection = collection_and_document[0].strip()
        document = collection_and_document[1].strip()
        fields = query.split('FETCH ')[1].split('.')
        ref_depth = len(fields)

        print('== FIRESTORE PROVIDER ============')
        print(f'COLLECTION: {collection}', flush=True)
        print(f'DOCUMENT: {document}', flush=True)
        print(f'FIELDS: {fields}', flush=True)
        print('==================================', flush=True)
        
        doc_ref = self.client.collection(collection).document(document).get()
        if doc_ref is None:
            return results
        
        firestore_document = doc_ref.to_dict()
        
        if ref_depth == 0:
            results = firestore_document
        if ref_depth == 1:
            results = firestore_document[fields[0]]
        elif ref_depth == 2:
            results = firestore_document[fields[0]][fields[1]]
        elif ref_depth == 3:
            results = firestore_document[fields[0]][fields[1]][fields[2]]
        elif ref_depth == 4:
            results = firestore_document[fields[0]][fields[1]][fields[2]][fields[3]]
        elif ref_depth == 5:
            results = firestore_document[fields[0]][fields[1]][fields[2]][fields[3]][fields[4]]

        return results

    def get_alerts(self, alert_id: Optional[str] = None):
        pass  # Define how to get alerts from Firestore if applicable

    def deploy_alert(self, alert: dict, alert_id: Optional[str] = None):
        pass  # Define how to deploy an alert to Firestore if applicable

    @staticmethod
    def get_alert_schema() -> dict:
        pass  # Return alert schema specific to Firestore

    def get_logs(self, limit: int = 5) -> list:
        pass  # Define how to get logs from Firestore if applicable

    def expose(self):
        return {}  # Define any parameters to expose

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    # If you want to use application default credentials, you can omit the authentication config
    config = {
        # "authentication": {"credentials_path": "/path/to/your/service_account.json"},
        "authentication": {},
    }

    # Create the provider
    provider = FirestoreProvider(
        provider_id="firestore-provider", config=ProviderConfig(**config)
    )

    # Use the provider to execute a query
    q = "FROM keep-test-collection.ZCA0JbCdgyxkMEm8VvmG FETCH test_field"
    results = provider.query(query=q)

    # Print the results
    print(results, flush=True)