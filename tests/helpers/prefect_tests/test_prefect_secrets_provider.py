import pytest
import os

from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

pytest.importorskip("prefect")

from dlt import TSecretValue
from dlt.common.configuration.specs import PrefectCredentials
from dlt.common.configuration.providers.prefect import PrefectSecretsProvider
from dlt.common.configuration.accessors import secrets
from dlt.common.configuration.specs.config_providers_context import _prefect_provider
from dlt.common.configuration.specs import GcpServiceAccountCredentials, known_sections
from dlt.common.typing import AnyType
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext

from tests.utils import init_test_logging

from prefect.blocks.system import Secret
from prefect.client.orchestration import SyncPrefectClient
from prefect.settings import temporary_settings, PREFECT_API_URL

import asyncio

DLT_SECRETS_TOML_CONTENT = """
[providers.prefect.credentials]
api_url = "http://localhost:4200/api"
"""


@pytest.fixture(scope="function", autouse=True)
def initialize_prefect_secrets():
    # backup context providers
    providers = Container()[PluggableRunContext].providers
    os.environ["PROVIDERS__ENABLE_PREFECT_SECRETS"] = "true"
    os.environ["PROVIDERS__PREFECT__CREDENTIALS__API_URL"] = "http://localhost:4200/api"

    secret = Secret(value=DLT_SECRETS_TOML_CONTENT.strip())

    key = SECRETS_TOML_KEY.replace("_", "-")

    with temporary_settings(updates={PREFECT_API_URL: "http://localhost:4200/api"}):
        secret.save(name=key, overwrite=True)
        # re-create providers
        Container()[PluggableRunContext].reload_providers()
        yield
        # restore providers
        Container()[PluggableRunContext].providers = providers


@pytest.mark.asyncio
async def test_regular_keys() -> None:
    init_test_logging()

    provider: PrefectSecretsProvider = _prefect_provider()  # type: ignore[assignment]

    assert provider.to_toml().strip() == DLT_SECRETS_TOML_CONTENT.strip()
