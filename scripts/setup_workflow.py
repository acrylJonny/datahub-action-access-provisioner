"""
Pre-step: create the DataHub Access Workflow before running the provisioner action.

This script must be run ONCE (or after any form-field change) before deploying
`datahub actions -c example_action.yaml`.  It creates (or overwrites) the
self-service access request workflow in DataHub Cloud via GraphQL.

What it creates
---------------
Workflow 1 — Snowflake Data Access Request (ACCESS category)
  Trigger  : form submission (FORM_SUBMITTED)
  Entrypoints:
    - ENTITY_PROFILE  → "Request Access" button on dataset/data-product pages
    - HOME            → "Request Access" tile on the home page
  Entity types: DATASET, DATA_PRODUCT
  Form fields (all match provisioner default field IDs):
    - snowflake_database   (STRING, required)
    - snowflake_schema     (STRING, optional)
    - snowflake_role       (STRING, required)
    - access_duration_days (NUMBER, optional)
    - requestor_email      (STRING, required)
    - justification        (RICH_TEXT, required)
  Approval step:
    - Dynamically assigned to ENTITY_OWNERS (dataset/product owner team)
    - Falls back to ENTITY_DOMAIN_OWNERS if no entity owners are set

Workflow 2 — New Data Product Request (CUSTOM category, optional)
  Same entrypoints + a simplified form for requesting creation of a new
  data product when none exists yet.  Routed to domain owners.
  Pass --skip-data-product-request to omit this workflow.

Usage
-----
    # From the repo root:
    pip install acryl-datahub requests
    export DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
    export DATAHUB_GMS_TOKEN=<token>
    python3 scripts/setup_workflow.py

    # Dry-run (prints the mutation variables without calling the API):
    python3 scripts/setup_workflow.py --dry-run

    # Skip the optional data-product request workflow:
    python3 scripts/setup_workflow.py --skip-data-product-request

    # Override field IDs to match a custom action.yaml:
    python3 scripts/setup_workflow.py \\
        --field-database my_db_field \\
        --field-role my_role_field \\
        --field-email my_email_field

Field ID alignment
------------------
The field `id` values here MUST match the `field_*` keys in your action.yaml.
Default mapping (can be overridden via CLI flags):

    action.yaml key          → form field id
    ─────────────────────────────────────────
    field_snowflake_database → snowflake_database
    field_snowflake_schema   → snowflake_schema
    field_snowflake_role     → snowflake_role
    field_access_duration_days → access_duration_days
    field_requestor_email    → requestor_email
    field_justification      → justification
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GraphQL mutation
# ---------------------------------------------------------------------------

_UPSERT_WORKFLOW_MUTATION = """
mutation upsertActionWorkflow($input: UpsertActionWorkflowInput!) {
  upsertActionWorkflow(input: $input) {
    urn
  }
}
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _graphql(gms_url: str, token: str, query: str, variables: dict) -> dict:
    url = gms_url.rstrip("/") + "/api/graphql"
    resp = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {json.dumps(body['errors'], indent=2)}")
    return body["data"]


def _upsert_workflow(
    gms_url: str,
    token: str,
    variables: dict,
    dry_run: bool,
) -> str | None:
    if dry_run:
        log.info("DRY RUN — mutation variables:\n%s", json.dumps(variables, indent=2))
        return None
    data = _graphql(gms_url, token, _UPSERT_WORKFLOW_MUTATION, variables)
    urn = data["upsertActionWorkflow"]["urn"]
    log.info("  Created/updated workflow: %s", urn)
    return urn


# ---------------------------------------------------------------------------
# Workflow 1 — Snowflake Data Access Request
# ---------------------------------------------------------------------------


def build_access_request_workflow(fields: dict[str, str]) -> dict:
    """
    Build the upsertActionWorkflow input for the Snowflake access request form.

    `fields` maps logical names to form field IDs, e.g.:
        {"database": "snowflake_database", "role": "snowflake_role", ...}
    """
    return {
        "input": {
            "name": "Snowflake Data Access Request",
            "category": "ACCESS",
            "description": (
                "Request access to a Snowflake dataset or data product. "
                "Your request will be reviewed by the dataset owner. "
                "Access is provisioned automatically on approval."
            ),
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    # Where the "Request Access" button appears
                    "entrypoints": [
                        {
                            "type": "ENTITY_PROFILE",
                            "label": "Request Access",
                        },
                        {
                            "type": "HOME",
                            "label": "Request Data Access",
                        },
                    ],
                    # Restrict to datasets and data products
                    "entityTypes": ["DATASET", "DATA_PRODUCT"],
                    # Form fields — IDs must match action.yaml field_* values
                    "fields": [
                        {
                            "id": fields["database"],
                            "name": "Snowflake Database",
                            "description": (
                                "The Snowflake database you need access to (e.g. PROD, ANALYTICS)."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": fields["schema"],
                            "name": "Snowflake Schema",
                            "description": (
                                "The schema within the database. Leave blank "
                                "to request access to all schemas."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": False,
                        },
                        {
                            "id": fields["role"],
                            "name": "Snowflake Role",
                            "description": (
                                "Your existing Snowflake role that should be "
                                "granted access (e.g. ANALYST_ROLE, "
                                "DATA_SCIENCE_ROLE)."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": fields["duration"],
                            "name": "Access Duration (days)",
                            "description": (
                                "How many days you need access for. Leave "
                                "blank for indefinite access (requires "
                                "additional justification)."
                            ),
                            "valueType": "NUMBER",
                            "cardinality": "SINGLE",
                            "required": False,
                        },
                        {
                            "id": fields["email"],
                            "name": "Requestor Email",
                            "description": (
                                "Your email address. Approval and denial "
                                "notifications will be sent here."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": fields["justification"],
                            "name": "Business Justification",
                            "description": (
                                "Describe your use case and why you need "
                                "access to this data. Include the project, "
                                "team, and data handling approach."
                            ),
                            "valueType": "RICH_TEXT",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                    ],
                },
            },
            # Approval step: routed to the entity owner team.
            # Falls back to domain owners if no entity owners are set.
            "steps": [
                {
                    "id": "owner-approval",
                    "type": "APPROVAL",
                    "description": (
                        "The dataset or data product owner reviews and "
                        "approves or denies the access request."
                    ),
                    "actors": {
                        "userUrns": [],
                        "groupUrns": [],
                        "roleUrns": [],
                        "dynamicAssignment": {
                            "type": "ENTITY_OWNERS",
                        },
                    },
                },
                {
                    "id": "domain-owner-fallback",
                    "type": "APPROVAL",
                    "description": (
                        "If no entity owners are set, the domain owner reviews the request."
                    ),
                    "actors": {
                        "userUrns": [],
                        "groupUrns": [],
                        "roleUrns": [],
                        "dynamicAssignment": {
                            "type": "ENTITY_DOMAIN_OWNERS",
                        },
                    },
                },
            ],
        }
    }


# ---------------------------------------------------------------------------
# Workflow 2 — New Data Product Request (optional)
# ---------------------------------------------------------------------------


def build_data_product_request_workflow() -> dict:
    """
    Build the upsertActionWorkflow input for requesting creation of a new
    data product when no suitable one exists in the catalog.
    """
    return {
        "input": {
            "name": "New Data Product Request",
            "category": "CUSTOM",
            "customCategory": "Data Product Creation",
            "description": (
                "Request the creation of a new data product when you cannot "
                "find the data you need in the catalog. Your request will be "
                "routed to the relevant domain owner."
            ),
            "trigger": {
                "type": "FORM_SUBMITTED",
                "form": {
                    "entrypoints": [
                        {
                            "type": "HOME",
                            "label": "Request New Data Product",
                        },
                    ],
                    "entityTypes": [],
                    "fields": [
                        {
                            "id": "product_name",
                            "name": "Proposed Data Product Name",
                            "description": (
                                "What should this data product be called? "
                                "Use a clear, business-friendly name."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "business_domain",
                            "name": "Business Domain",
                            "description": (
                                "Which domain does this data belong to? "
                                "(e.g. Consumer, SME & Enterprise, Finance)."
                            ),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "use_case",
                            "name": "Use Case & Requirements",
                            "description": (
                                "Describe your use case, the KPIs or "
                                "analysis you need to perform, and any "
                                "specific data requirements "
                                "(e.g. customer segment, time period, "
                                "refresh cadence)."
                            ),
                            "valueType": "RICH_TEXT",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                        {
                            "id": "priority",
                            "name": "Priority",
                            "description": "How urgently do you need this?",
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                            "allowedValues": [
                                {"stringValue": "Low — nice to have"},
                                {"stringValue": "Medium — needed within a month"},
                                {"stringValue": "High — blocking current work"},
                                {"stringValue": "Critical — executive deliverable"},
                            ],
                        },
                        {
                            "id": "requestor_email",
                            "name": "Requestor Email",
                            "description": ("Your email address for status notifications."),
                            "valueType": "STRING",
                            "cardinality": "SINGLE",
                            "required": True,
                        },
                    ],
                },
            },
            "steps": [
                {
                    "id": "domain-owner-review",
                    "type": "APPROVAL",
                    "description": (
                        "The domain owner reviews the request and assigns "
                        "it to the appropriate data engineering team."
                    ),
                    "actors": {
                        "userUrns": [],
                        "groupUrns": [],
                        "roleUrns": [],
                        "dynamicAssignment": {
                            "type": "ENTITY_DOMAIN_OWNERS",
                        },
                    },
                },
            ],
        }
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create/update DataHub Access Workflows before running the provisioner action.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print mutation variables without calling the DataHub API.",
    )
    parser.add_argument(
        "--skip-data-product-request",
        action="store_true",
        help="Skip creation of the optional 'New Data Product Request' workflow.",
    )
    # Allow field ID overrides to match a custom action.yaml
    parser.add_argument("--field-database", default="snowflake_database")
    parser.add_argument("--field-schema", default="snowflake_schema")
    parser.add_argument("--field-role", default="snowflake_role")
    parser.add_argument("--field-duration", default="access_duration_days")
    parser.add_argument("--field-email", default="requestor_email")
    parser.add_argument("--field-justification", default="justification")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    gms_url = os.environ.get("DATAHUB_GMS_URL", "")
    token = os.environ.get("DATAHUB_GMS_TOKEN", "")

    if not args.dry_run:
        if not gms_url:
            sys.exit("ERROR: DATAHUB_GMS_URL environment variable is not set.")
        if not token:
            sys.exit("ERROR: DATAHUB_GMS_TOKEN environment variable is not set.")

    field_ids = {
        "database": args.field_database,
        "schema": args.field_schema,
        "role": args.field_role,
        "duration": args.field_duration,
        "email": args.field_email,
        "justification": args.field_justification,
    }

    log.info("Field ID mapping:")
    for logical, form_id in field_ids.items():
        log.info("  %-20s → %s", logical, form_id)

    # ── Workflow 1: Snowflake Data Access Request ──────────────────────────
    log.info("")
    log.info("Creating workflow: Snowflake Data Access Request…")
    variables_1 = build_access_request_workflow(field_ids)
    urn_1 = _upsert_workflow(gms_url, token, variables_1, args.dry_run)
    if urn_1:
        log.info("  Access request workflow ready.")

    # ── Workflow 2: New Data Product Request (optional) ────────────────────
    if not args.skip_data_product_request:
        log.info("")
        log.info("Creating workflow: New Data Product Request…")
        variables_2 = build_data_product_request_workflow()
        urn_2 = _upsert_workflow(gms_url, token, variables_2, args.dry_run)
        if urn_2:
            log.info("  Data product request workflow ready.")

    log.info("")
    log.info("Done. You can now deploy the provisioner action:")
    log.info("  datahub actions -c examples/example_action.yaml")

    if not args.dry_run:
        log.info("")
        log.info("Verify in the DataHub UI:")
        gms_base = gms_url.replace("/gms", "")
        log.info("  Settings → Workflows → %s", gms_base)


if __name__ == "__main__":
    main()
