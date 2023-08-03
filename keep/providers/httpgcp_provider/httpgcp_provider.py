"""
HttpProvider is a class that provides a way to send HTTP requests.
"""
import typing

import requests
from requests.exceptions import JSONDecodeError

from keep.providers.base.base_provider import BaseProvider
from keep.providers.models.provider_config import ProviderConfig


class HttpgcpProvider(BaseProvider):
    def __init__(self, provider_id: str, config: ProviderConfig):
        super().__init__(provider_id, config)

    def dispose(self):
        """
        Nothing to do here.
        """
        pass

    def validate_config(self):
        """
        No configuration to validate here
        """

    def notify(
        self,
        url: str,
        method: typing.Literal["GET", "POST", "PUT", "DELETE"],
        headers: dict = None,
        body: dict = None,
        params: dict = None,
        proxies: dict = None,
        **kwargs,
    ):
        """
        Send a HTTP request to the given url.
        """
        self.query(
            url=url,
            method=method,
            headers=headers,
            body=body,
            params=params,
            proxies=proxies,
            **kwargs,
        )

    def _query(
        self,
        url: str,
        method: typing.Literal["GET", "POST", "PUT", "DELETE"],
        headers: dict = None,
        body: dict = None,
        params: dict = None,
        proxies: dict = None,
        **kwargs: dict,
    ) -> dict:
        """
        Send a HTTP request to the given url using Google's Metadata Server.
        """
        import os

        if headers is None:
            headers = {}
        if body is None:
            body = {}
        if params is None:
            params = {}

        # Use Google's Metadata Server
        is_local_environment = os.environ.get("X_PA_GCP_JWT", None)
        if is_local_environment is None:
            # Set up metadata server request
            # See https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
            metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

            token_request_url = metadata_server_token_url + url
            token_request_headers = {'Metadata-Flavor': 'Google'}

            # Fetch the token
            token_response = requests.get(token_request_url, headers=token_request_headers)
            jwt = token_response.content.decode("utf-8")
            headers['Authorization'] = f'Bearer {jwt}'
        else:
            headers['Authorization'] = f'Bearer {is_local_environment}'

        if method == "GET":
            response = requests.get(
                url, headers=headers, params=params, proxies=proxies, **kwargs
            )
        elif method == "POST":
            response = requests.post(
                url, headers=headers, json=body, proxies=proxies, **kwargs
            )
        elif method == "PUT":
            response = requests.put(
                url, headers=headers, json=body, proxies=proxies, **kwargs
            )
        elif method == "DELETE":
            response = requests.delete(
                url, headers=headers, json=body, proxies=proxies, **kwargs
            )
        else:
            raise Exception(f"Unsupported HTTP method: {method}")

        result = {"status": response.ok, "status_code": response.status_code}

        try:
            body = response.json()
        except JSONDecodeError:
            body = response.text

        result["body"] = body
        print('== HTTP PROVIDER ===============================', flush=True)
        print({
                "url": url,
                "method": method,
                "body": body,
                "headers": headers,
                "params": params
            }, flush=True)
        print(result, flush=True)
        print("================================================", flush=True)
        return result
