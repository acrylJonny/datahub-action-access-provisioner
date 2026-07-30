import time

import pytest
from datahub.configuration.common import AllowDenyPattern

from action_access_provisioner.config import WorkflowFilterConfig
from action_access_provisioner.models import AccessRequest, FormFieldValues

DATASET = "urn:li:dataset:(urn:li:dataPlatform:databricks,prod.sales.orders,PROD)"


def _request(*, workflow_name=None, workflow_urn=None, **form) -> AccessRequest:
    return AccessRequest(
        urn="urn:li:actionRequest:test",
        status="COMPLETED",
        result="ACCEPTED",
        note=None,
        request_type="WORKFLOW_FORM_REQUEST",
        workflow_urn=workflow_urn,
        workflow_name=workflow_name,
        resource=DATASET,
        requestor_urn="urn:li:corpuser:alice@example.com",
        created_ms=int(time.time() * 1000),
        due_date_ms=None,
        form_fields=FormFieldValues(**form),
    )


ACCESS = _request(workflow_name="Dataset Access Request", access_duration_days=30)
# A revocation request targets the same dataset and is also a workflow form
# request; only its form content distinguishes it from an access request.
REVOCATION = _request(workflow_name="Access Revocation Request")


def test_access_request_is_permitted_by_default():
    assert WorkflowFilterConfig().permits(ACCESS) is True


def test_non_access_workflow_is_rejected_without_any_configuration():
    # The dangerous default: approving a revocation must never grant access,
    # even in a deployment that has not configured an allow/deny pattern.
    assert WorkflowFilterConfig().permits(REVOCATION) is False


def test_group_only_request_counts_as_an_access_request():
    grouped = _request(workflow_name="Dataset Access Request", databricks_group="analytics")
    assert WorkflowFilterConfig().permits(grouped) is True


def test_require_access_fields_can_be_disabled():
    cfg = WorkflowFilterConfig(require_access_fields=False)
    assert cfg.permits(REVOCATION) is True


@pytest.mark.parametrize(
    "pattern,expected",
    [
        (AllowDenyPattern(allow=["Dataset Access Request"]), True),
        (AllowDenyPattern(allow=["Data Product .*"]), False),
        (AllowDenyPattern(deny=["Dataset Access Request"]), False),
    ],
)
def test_pattern_matches_on_workflow_name(pattern, expected):
    cfg = WorkflowFilterConfig(workflow=pattern)
    assert cfg.permits(ACCESS) is expected


def test_pattern_matches_on_workflow_urn():
    req = _request(
        workflow_urn="urn:li:actionWorkflow:abc-123",
        access_duration_days=30,
    )
    cfg = WorkflowFilterConfig(workflow=AllowDenyPattern(allow=["urn:li:actionWorkflow:abc-123"]))
    assert cfg.permits(req) is True


def test_deny_on_one_identifier_is_not_readmitted_by_the_other():
    req = _request(
        workflow_name="Dataset Access Request",
        workflow_urn="urn:li:actionWorkflow:abc-123",
        access_duration_days=30,
    )
    cfg = WorkflowFilterConfig(
        workflow=AllowDenyPattern(
            allow=["Dataset Access Request"], deny=["urn:li:actionWorkflow:abc-123"]
        )
    )
    assert cfg.permits(req) is False


def test_missing_workflow_identity_is_allowed_only_while_pattern_is_default():
    # Older GMS builds do not return the workflow on the request.
    unknown = _request(access_duration_days=30)
    assert WorkflowFilterConfig().permits(unknown) is True

    explicit = WorkflowFilterConfig(workflow=AllowDenyPattern(allow=["Dataset Access Request"]))
    assert explicit.permits(unknown) is False
