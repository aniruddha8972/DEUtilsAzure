"""
de_utils.secrets
----------------
Secure secret resolution — never put credentials in code or config files.

Supports:
    AzureKeyVaultSecrets  — fetch secrets from Azure Key Vault
    EnvSecrets            — read from environment variables (dev/CI)
    SecretsChain          — try multiple backends in order (Key Vault → env → default)
    resolve_config        — recursively replace ${SECRET:name} placeholders in dicts

Usage
-----
>>> from de_utils.secrets import SecretsChain, AzureKeyVaultSecrets, EnvSecrets

>>> secrets = SecretsChain([
...     AzureKeyVaultSecrets(vault_url="https://my-vault.vault.azure.net/"),
...     EnvSecrets(prefix="DE_"),
... ])

>>> db_password = secrets.get("db-password")          # Key Vault first, then env DE_DB_PASSWORD
>>> adls_key    = secrets.get("adls-account-key")

>>> # Resolve ${SECRET:name} placeholders in any dict (e.g. loaded from YAML)
>>> raw_cfg = {
...     "account_key": "${SECRET:adls-account-key}",
...     "jdbc_password": "${SECRET:jdbc-password}",
... }
>>> resolved = secrets.resolve_dict(raw_cfg)
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from .utils import get_logger, DataEngineeringError

log = get_logger(__name__)

_PLACEHOLDER_RE = re.compile(r"\$\{SECRET:([^}]+)\}")


# ── Base ──────────────────────────────────────────────────────────────────────

class BaseSecrets:
    """Abstract base for secret backends."""

    name: str = "base"

    def get(self, key: str) -> Optional[str]:
        raise NotImplementedError

    def get_required(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise DataEngineeringError(
                f"[{self.name}] Required secret '{key}' not found."
            )
        return value


# ── Azure Key Vault ───────────────────────────────────────────────────────────

class AzureKeyVaultSecrets(BaseSecrets):
    """
    Fetch secrets from Azure Key Vault using DefaultAzureCredential
    (supports Managed Identity, service principal, VS Code login, etc.)

    Requires: pip install azure-keyvault-secrets azure-identity

    Example
    -------
    >>> kv = AzureKeyVaultSecrets("https://my-vault.vault.azure.net/")
    >>> password = kv.get("sql-password")
    """

    name = "azure_keyvault"

    def __init__(self, vault_url: str, cache: bool = True):
        self.vault_url  = vault_url.rstrip("/")
        self._cache     = {} if cache else None
        self._client    = None

    def _get_client(self):
        if self._client is None:
            try:
                from azure.keyvault.secrets import SecretClient
                from azure.identity import DefaultAzureCredential
            except ImportError as e:
                raise ImportError(
                    "Install azure-keyvault-secrets and azure-identity: "
                    "pip install azure-keyvault-secrets azure-identity"
                ) from e
            self._client = SecretClient(
                vault_url=self.vault_url,
                credential=DefaultAzureCredential(),
            )
        return self._client

    def get(self, key: str) -> Optional[str]:
        if self._cache is not None and key in self._cache:
            return self._cache[key]
        try:
            secret = self._get_client().get_secret(key)
            value  = secret.value
            if self._cache is not None:
                self._cache[key] = value
            log.debug("[keyvault] Resolved secret '%s'", key)
            return value
        except Exception as exc:
            if "SecretNotFound" in type(exc).__name__ or "404" in str(exc):
                return None
            log.warning("[keyvault] Could not fetch secret '%s': %s", key, exc)
            return None

    def set(self, key: str, value: str, content_type: str = "text/plain") -> None:
        """Write a secret to Key Vault (useful for bootstrapping)."""
        self._get_client().set_secret(key, value, content_type=content_type)
        if self._cache is not None:
            self._cache[key] = value
        log.info("[keyvault] Secret '%s' written.", key)

    def delete(self, key: str) -> None:
        self._get_client().begin_delete_secret(key)
        if self._cache is not None:
            self._cache.pop(key, None)
        log.info("[keyvault] Secret '%s' deleted.", key)

    def list_secrets(self) -> List[str]:
        return [s.name for s in self._get_client().list_properties_of_secrets()]

    def clear_cache(self) -> None:
        if self._cache is not None:
            self._cache.clear()


# ── Environment variables ─────────────────────────────────────────────────────

class EnvSecrets(BaseSecrets):
    """
    Read secrets from environment variables.
    Key 'my-secret' maps to env var 'PREFIX_MY_SECRET' (hyphens → underscores, uppercased).

    Example
    -------
    >>> env = EnvSecrets(prefix="DE_")
    >>> env.get("adls-account-key")   # reads DE_ADLS_ACCOUNT_KEY
    """

    name = "env"

    def __init__(self, prefix: str = ""):
        self.prefix = prefix.upper()

    def _env_key(self, key: str) -> str:
        return (self.prefix + key.upper().replace("-", "_").replace(".", "_"))

    def get(self, key: str) -> Optional[str]:
        env_key = self._env_key(key)
        value   = os.environ.get(env_key)
        if value is not None:
            log.debug("[env] Resolved secret '%s' from %s", key, env_key)
        return value

    def set_for_testing(self, key: str, value: str) -> None:
        """Inject a secret into the environment (use in tests only)."""
        os.environ[self._env_key(key)] = value


# ── Static / hardcoded (dev only) ─────────────────────────────────────────────

class StaticSecrets(BaseSecrets):
    """
    In-memory secret store for testing. Never use in production.

    Example
    -------
    >>> static = StaticSecrets({"db-password": "test123"})
    """

    name = "static"

    def __init__(self, secrets: Optional[Dict[str, str]] = None):
        self._store: Dict[str, str] = secrets or {}

    def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value


# ── SecretsChain ──────────────────────────────────────────────────────────────

class SecretsChain(BaseSecrets):
    """
    Try multiple secret backends in order, returning the first non-None value.

    Example
    -------
    >>> secrets = SecretsChain([
    ...     AzureKeyVaultSecrets("https://my-vault.vault.azure.net/"),
    ...     EnvSecrets("DE_"),
    ...     StaticSecrets({"dev-only-key": "dev-value"}),   # fallback
    ... ])
    >>> password = secrets.get("db-password")
    """

    name = "chain"

    def __init__(self, backends: List[BaseSecrets]):
        self.backends = backends

    def get(self, key: str) -> Optional[str]:
        for backend in self.backends:
            try:
                value = backend.get(key)
                if value is not None:
                    return value
            except Exception as exc:
                log.debug("[chain] Backend '%s' error for key '%s': %s", backend.name, key, exc)
        return None

    # ------------------------------------------------------------------
    # Placeholder resolution
    # ------------------------------------------------------------------

    def resolve_value(self, value: Any) -> Any:
        """Resolve ${SECRET:name} placeholders in a single value."""
        if not isinstance(value, str):
            return value
        def replace(match):
            key       = match.group(1)
            resolved  = self.get(key)
            if resolved is None:
                raise DataEngineeringError(
                    f"Secret placeholder '${{SECRET:{key}}}' could not be resolved "
                    f"from any backend in the chain."
                )
            return resolved
        return _PLACEHOLDER_RE.sub(replace, value)

    def resolve_dict(self, cfg: Dict, recursive: bool = True) -> Dict:
        """
        Recursively replace ${SECRET:name} in all string values of a dict.

        Example
        -------
        >>> cfg = {"password": "${SECRET:db-password}", "host": "mydb.azure.com"}
        >>> resolved = secrets.resolve_dict(cfg)
        >>> resolved["password"]   # actual password
        """
        result = {}
        for k, v in cfg.items():
            if isinstance(v, dict) and recursive:
                result[k] = self.resolve_dict(v)
            elif isinstance(v, list):
                result[k] = [
                    self.resolve_dict(item) if isinstance(item, dict)
                    else self.resolve_value(item)
                    for item in v
                ]
            else:
                result[k] = self.resolve_value(v)
        return result
