import io
import contextlib

from prefect.exceptions import ObjectNotFound

from dlt.common.configuration.specs.base_configuration import is_secret_hint
from dlt.common.configuration.specs.prefect_credentials import PrefectCredentials
from dlt.common.exceptions import MissingDependencyException

from .vault import VaultDocProvider


class PrefectSecretsProvider(VaultDocProvider):
    def __init__(
        self,
        credentials: PrefectCredentials,
        only_toml_fragments: bool = False,
    ) -> None:
        self.credentials = credentials
        super().__init__(only_secrets=True, only_toml_fragments=only_toml_fragments)

    @property
    def name(self) -> str:
        return "Prefect Secrets Provider"

    def _look_vault(self, full_key: str, hint: type) -> str:
        try:
            from prefect.client.orchestration import SyncPrefectClient
            from prefect.blocks.system import Secret
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "PrefectSecretsProvider",
                ["prefect-client"],
                "We need prefect-client to build a client for Prefect",
            )

        # Prefect doesn't support underscores, so convert to dashes
        full_key = full_key.replace("_", "-")

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()
        ), SyncPrefectClient(
            self.credentials.api_url,
            auth_string=self.credentials.auth_string,
        ) as client:
            try:
                doc = client.read_block_document_by_name(
                    name=full_key, block_type_slug=Secret.get_block_type_slug()
                )
                secret = Secret._from_block_document(doc)
            except ObjectNotFound:
                return None
            return secret.value.get_secret_value()

    @property
    def supports_secrets(self) -> bool:
        return True
