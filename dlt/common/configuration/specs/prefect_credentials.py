from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
import os
from dlt.common.typing import Optional


@configspec
class PrefectCredentials(CredentialsConfiguration, CredentialsWithDefault):
    api_url: str = None
    auth_string: Optional[str] = None

    @staticmethod
    def _get_default_credentials():
        url = os.getenv("PREFECT_API_URL", None)
        auth = os.getenv("PREFECT_API_AUTH_STRING", None)
        return url, auth

    def on_partial(self) -> None:
        prefect_api_url, prefect_auth_string = PrefectCredentials._get_default_credentials()
        if prefect_api_url is None:
            return

        self.api_url = prefect_api_url
        self.auth_string = prefect_auth_string
        self._set_default_credentials((prefect_api_url, prefect_auth_string))

        self.resolve()
