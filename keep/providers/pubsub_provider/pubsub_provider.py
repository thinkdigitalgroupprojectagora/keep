"""
DiscordProvider is a class that implements the BaseOutputProvider interface for Discord messages.
"""
import dataclasses

import pydantic

from keep.exceptions.provider_exception import ProviderException
from keep.providers.base.base_provider import BaseProvider
from keep.providers.models.provider_config import ProviderConfig

from google.cloud import pubsub_v1

@pydantic.dataclasses.dataclass
class PubsubProviderAuthConfig:
    """Google PubSub authentication configuration."""

    topic_id: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Google topic_id",
            "sensitive": True,
        }
    )
    project_id: str = dataclasses.field(
        metadata={
            "required": True,
            "description": "Google project_id",
            "sensitive": True,
        }
    )


class PubsubProvider(BaseProvider):
    def __init__(self, provider_id: str, config: ProviderConfig):
        super().__init__(provider_id, config)

    def validate_config(self):
        self.authentication_config = PubsubProviderAuthConfig(
            **self.config.authentication
        )

    def dispose(self):
        """
        No need to dispose of anything, so just do nothing.
        """
        pass

    def notify(self, **kwargs: dict):
        """
        Publish a message in Google PubSub

        Args:
            kwargs (dict): The providers with context
        """
        import json

        self.logger.debug("Notifying Google PubSub")
        topic_id = self.authentication_config.topic_id
        project_id = self.authentication_config.project_id
        message = kwargs.pop("message", "")
        event_type = kwargs.pop("event_type_attr", "")

        if not message:
            raise ProviderException(
                f"{self.__class__.__name__} Keyword Arguments Missing : 'message' keyword is needed to trigger PubSub message"
            )
        
        # actual pubsub intergration here
        data = json.dumps(message).encode('utf-8')
        publisher = pubsub_v1.PublisherClient()
        publisher.publish(topic_id, data, event_type=event_type)
        
        self.logger.debug("Google PubSub triggered")

        print('== PUBSUB PROVIDER ===============')
        print(f'TOPIC ID: {topic_id}', flush=True)
        print(f'PROJECT ID: {project_id}', flush=True)
        print(f'MESSAGE: ', flush=True)
        print(json.loads(message), flush=True)
        print('==================================', flush=True)

if __name__ == "__main__":
    # Output debug messages
    import logging

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    # Load environment variables
    import os

    topic_id = "my-topic-id"
    project_id = 'my-project-id-123'
    message = "{'message': 'pubsub message}"

    # Initalize the provider and provider config
    config = ProviderConfig(
        description="Discord Output Provider",
        authentication={"topic_id": topic_id, 'project_id': project_id}
    )
    provider = PubsubProvider(provider_id="pubsub-test", config=config)

    provider.notify(
        message=message
    )
