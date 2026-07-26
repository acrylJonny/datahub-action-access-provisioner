"""Fetch an OAuth access token for Snowflake ``OAUTH_AUTHENTICATOR``.

Mirrors the DataHub Snowflake ingestion connector's OAuth token generation so the
same ``oauth_config`` works here: Microsoft (via MSAL, secret or certificate) and
Okta (client-credentials, or password grant when a username/password is supplied).
"""

import base64
import logging

from action_access_provisioner.config import OAuthConfiguration, OAuthIdentityProvider

logger = logging.getLogger(__name__)

_GRANT_CLIENT_CREDENTIALS = "client_credentials"
_GRANT_PASSWORD = "password"


def generate_oauth_token(
    oauth_config: OAuthConfiguration,
    *,
    username: str | None,
    password: str | None,
) -> str:
    """Return an OAuth access token for the configured provider."""
    if oauth_config.provider == OAuthIdentityProvider.MICROSOFT:
        return _microsoft_token(oauth_config)
    return _okta_token(oauth_config, username=username, password=password)


def _microsoft_token(oauth_config: OAuthConfiguration) -> str:
    import msal  # type: ignore[import-untyped]

    credential: str | dict | None
    if oauth_config.use_certificate:
        credential = _certificate_credential(oauth_config)
    else:
        credential = oauth_config.client_secret

    app = msal.ConfidentialClientApplication(
        oauth_config.client_id,
        authority=oauth_config.authority_url,
        client_credential=credential,
    )
    # Try the token cache first, then request a fresh one.
    result = app.acquire_token_silent(oauth_config.scopes, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=oauth_config.scopes)
    if "access_token" not in result:
        raise ValueError(
            f"Microsoft OAuth token request failed: "
            f"{result.get('error')}: {result.get('error_description')}"
        )
    return str(result["access_token"])


def _certificate_credential(oauth_config: OAuthConfiguration) -> dict:
    """Build the MSAL client_credential dict from base64-encoded cert material."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.x509 import load_pem_x509_certificate

    assert oauth_config.encoded_oauth_private_key is not None
    assert oauth_config.encoded_oauth_public_key is not None
    private_key_pem = base64.b64decode(oauth_config.encoded_oauth_private_key)
    public_cert_pem = base64.b64decode(oauth_config.encoded_oauth_public_key)
    cert = load_pem_x509_certificate(public_cert_pem, default_backend())
    thumbprint = cert.fingerprint(hashes.SHA1()).hex()
    return {
        "private_key": private_key_pem.decode(),
        "thumbprint": thumbprint,
        "public_certificate": public_cert_pem.decode(),
    }


def _okta_token(
    oauth_config: OAuthConfiguration,
    *,
    username: str | None,
    password: str | None,
) -> str:
    import requests

    data = {"scope": " ".join(oauth_config.scopes)}
    # A username+password pair switches Okta to the resource-owner password grant;
    # otherwise use client_credentials (mirrors the ingestion connector).
    if username and password:
        data["grant_type"] = _GRANT_PASSWORD
        data["username"] = username
        data["password"] = password
    else:
        data["grant_type"] = _GRANT_CLIENT_CREDENTIALS

    resp = requests.post(
        oauth_config.authority_url,
        data=data,
        auth=(oauth_config.client_id, oauth_config.client_secret or ""),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "access_token" not in payload:
        raise ValueError(f"Okta OAuth token request returned no access_token: {payload}")
    return str(payload["access_token"])
