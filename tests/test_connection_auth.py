from unittest.mock import MagicMock, patch

import pytest

from action_access_provisioner.config import (
    AzureAuthConfig,
    DatabricksConnectionConfig,
    OAuthConfiguration,
    OAuthIdentityProvider,
    SnowflakeConnectionConfig,
)

_OKTA = OAuthIdentityProvider.OKTA

# ===========================================================================
# Snowflake auth
# ===========================================================================


def _rsa_private_key_pem() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def test_snowflake_rejects_unknown_auth_type():
    with pytest.raises(ValueError):
        SnowflakeConnectionConfig(account_id="acc", authentication_type="MAGIC")


def test_snowflake_key_pair_requires_a_key():
    with pytest.raises(ValueError):
        SnowflakeConnectionConfig(
            account_id="acc", username="u", authentication_type="KEY_PAIR_AUTHENTICATOR"
        )


def test_snowflake_private_key_requires_key_pair_auth():
    with pytest.raises(ValueError):
        SnowflakeConnectionConfig(account_id="acc", username="u", private_key="x")


def test_snowflake_token_only_valid_with_token_auth():
    with pytest.raises(ValueError):
        SnowflakeConnectionConfig(account_id="acc", username="u", token="t")


def test_snowflake_oauth_requires_oauth_config():
    with pytest.raises(ValueError):
        SnowflakeConnectionConfig(
            account_id="acc", username="u", authentication_type="OAUTH_AUTHENTICATOR"
        )


def test_oauth_config_certificate_is_microsoft_only():
    with pytest.raises(ValueError):
        OAuthConfiguration(
            provider=_OKTA,
            authority_url="https://okta/token",
            client_id="c",
            scopes=["s"],
            use_certificate=True,
            encoded_oauth_public_key="pub",
            encoded_oauth_private_key="priv",
        )


def test_snowflake_default_connect_passes_password_no_authenticator():
    cfg = SnowflakeConnectionConfig(account_id="acc", username="u", password="p", role="R")
    with patch("snowflake.connector.connect") as connect:
        cfg.get_native_connection()
    kwargs = connect.call_args.kwargs
    assert kwargs["password"] == "p"
    assert kwargs["account"] == "acc"
    assert kwargs["host"] == "acc.snowflakecomputing.com"
    assert kwargs["role"] == "R"
    assert "authenticator" not in kwargs


def test_snowflake_external_browser_sets_authenticator():
    cfg = SnowflakeConnectionConfig(
        account_id="acc", username="u", authentication_type="EXTERNAL_BROWSER_AUTHENTICATOR"
    )
    with patch("snowflake.connector.connect") as connect:
        cfg.get_native_connection()
    assert connect.call_args.kwargs["authenticator"] == "externalbrowser"


def test_snowflake_oauth_token_connects_with_token():
    cfg = SnowflakeConnectionConfig(
        account_id="acc",
        username="u",
        authentication_type="OAUTH_AUTHENTICATOR_TOKEN",
        token="tok",
    )
    with patch("snowflake.connector.connect") as connect:
        cfg.get_native_connection()
    kwargs = connect.call_args.kwargs
    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "tok"


def test_snowflake_key_pair_passes_der_private_key():
    cfg = SnowflakeConnectionConfig(
        account_id="acc",
        username="u",
        authentication_type="KEY_PAIR_AUTHENTICATOR",
        private_key=_rsa_private_key_pem(),
    )
    with patch("snowflake.connector.connect") as connect:
        cfg.get_native_connection()
    assert isinstance(connect.call_args.kwargs["private_key"], bytes)


def test_snowflake_oauth_authenticator_fetches_token_then_connects():
    cfg = SnowflakeConnectionConfig(
        account_id="acc",
        username="u",
        authentication_type="OAUTH_AUTHENTICATOR",
        oauth_config=OAuthConfiguration(
            provider=_OKTA,
            authority_url="https://okta/token",
            client_id="c",
            scopes=["s"],
            client_secret="secret",
        ),
    )
    with (
        patch(
            "action_access_provisioner.snowflake_oauth.generate_oauth_token",
            return_value="fetched-token",
        ),
        patch("snowflake.connector.connect") as connect,
    ):
        cfg.get_native_connection()
    kwargs = connect.call_args.kwargs
    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "fetched-token"


def test_snowflake_oauth_okta_client_credentials():
    from action_access_provisioner.snowflake_oauth import generate_oauth_token

    oauth = OAuthConfiguration(
        provider=_OKTA,
        authority_url="https://okta/token",
        client_id="cid",
        scopes=["session:role:R"],
        client_secret="secret",
    )
    resp = MagicMock()
    resp.json.return_value = {"access_token": "abc"}
    with patch("requests.post", return_value=resp) as post:
        token = generate_oauth_token(oauth, username=None, password=None)
    assert token == "abc"
    assert post.call_args.kwargs["data"]["grant_type"] == "client_credentials"


def test_snowflake_oauth_okta_password_grant_when_credentials_present():
    from action_access_provisioner.snowflake_oauth import generate_oauth_token

    oauth = OAuthConfiguration(
        provider=_OKTA,
        authority_url="https://okta/token",
        client_id="cid",
        scopes=["s"],
        client_secret="secret",
    )
    resp = MagicMock()
    resp.json.return_value = {"access_token": "abc"}
    with patch("requests.post", return_value=resp) as post:
        generate_oauth_token(oauth, username="u", password="p")
    assert post.call_args.kwargs["data"]["grant_type"] == "password"


# ===========================================================================
# Databricks auth
# ===========================================================================


def test_databricks_rejects_multiple_auth_methods():
    with pytest.raises(ValueError):
        DatabricksConnectionConfig(
            host="https://h", http_path="/p", token="t", client_id="c", client_secret="s"
        )


def test_databricks_oauth_requires_both_id_and_secret():
    with pytest.raises(ValueError):
        DatabricksConnectionConfig(host="https://h", http_path="/p", client_id="c")


def test_databricks_requires_http_path_or_warehouse_id():
    with pytest.raises(ValueError):
        DatabricksConnectionConfig(host="https://h", token="t")


def test_databricks_unified_auth_allowed():
    cfg = DatabricksConnectionConfig(host="https://h", http_path="/p")
    assert cfg.resolved_http_path == "/p"


def test_databricks_resolved_http_path_from_warehouse_id():
    cfg = DatabricksConnectionConfig(host="https://h", warehouse_id="wh123", token="t")
    assert cfg.resolved_http_path == "/sql/1.0/warehouses/wh123"


def test_databricks_workspace_client_token():
    cfg = DatabricksConnectionConfig(host="https://h", http_path="/p", token="tok")
    with patch("databricks.sdk.WorkspaceClient") as wc:
        cfg.get_workspace_client()
    kwargs = wc.call_args.kwargs
    assert kwargs["token"] == "tok"
    assert kwargs["host"] == "https://h"
    assert "azure_tenant_id" not in kwargs


def test_databricks_workspace_client_azure():
    cfg = DatabricksConnectionConfig(
        host="https://h",
        http_path="/p",
        azure_auth=AzureAuthConfig(client_id="ac", tenant_id="at", client_secret="asec"),
    )
    with patch("databricks.sdk.WorkspaceClient") as wc:
        cfg.get_workspace_client()
    kwargs = wc.call_args.kwargs
    assert kwargs["azure_tenant_id"] == "at"
    assert kwargs["azure_client_id"] == "ac"
    assert kwargs["azure_client_secret"] == "asec"


def test_databricks_sql_connection_uses_credentials_provider():
    cfg = DatabricksConnectionConfig(host="https://h", warehouse_id="wh123", token="tok")
    with (
        patch("databricks.sdk.WorkspaceClient"),
        patch("databricks.sql.connect") as connect,
    ):
        cfg.get_sql_connection()
    kwargs = connect.call_args.kwargs
    assert kwargs["server_hostname"] == "h"
    assert kwargs["http_path"] == "/sql/1.0/warehouses/wh123"
    assert callable(kwargs["credentials_provider"])
