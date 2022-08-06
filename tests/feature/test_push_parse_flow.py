from fastapi.testclient import TestClient
from tests.feature.test_data.unparsed_manifests import get_unparsed_manifest


def test_push_parse_flow(test_client: TestClient):
    """
    Test that the expected flow for getting a project setup in dbt-server works.
    Push then parse.
    TODO: How do we deal with installing dependencies in test?
        Probably, just don't test installing dependencies
        Instead, have a manifest.json that has dependencies
        This will cause dbt-server to have all the deps represented on its copy of the FS?
    """
    manifest = get_unparsed_manifest("jaffle_shop_metrics")

    response = test_client.post(
        "/push", json={"state_id": "abc-123", "install_deps": False, "body": manifest}
    )
    assert response.status_code == 200
    assert response.json() == {
        "bytes": 29,
        "path": "./working-dir/state-abc-123",
        "reuse": False,
        "state": "abc-123",
    }
