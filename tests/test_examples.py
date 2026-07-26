from pathlib import Path
from typing import Any

import yaml

from action_access_provisioner.config import (
    AccessProvisionerConfig,
    DatabricksAccessProvisionerConfig,
)

_EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def _action_spec_config(yaml_path: Path) -> dict[str, Any]:
    doc = yaml.safe_load(yaml_path.read_text())
    config: dict[str, Any] = doc["source"]["config"]["action_spec"]["config"]
    return config


def test_snowflake_example_config_is_valid():
    """The shipped Snowflake example must parse — it is copy-pasted into real
    deployments, so a typo'd key here breaks onboarding."""
    cfg = _action_spec_config(_EXAMPLES / "example_action.yaml")
    parsed = AccessProvisionerConfig.model_validate(cfg)
    assert parsed.reconcile.enabled is True
    assert parsed.reconcile.interval_seconds == 300
    assert parsed.state.ledger_table == "ACCESS_PROVISIONER_LEDGER"


def test_databricks_example_config_is_valid():
    cfg = _action_spec_config(_EXAMPLES / "example_action_databricks.yaml")
    parsed = DatabricksAccessProvisionerConfig.model_validate(cfg)
    assert parsed.reconcile.enabled is True
    assert parsed.reconcile.interval_seconds == 300
    assert parsed.state.ledger_table == "access_provisioner_ledger"
