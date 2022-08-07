from tests.feature.context import test_context
from tests.feature.test_data.unparsed_manifests import get_unparsed_manifest


def test_push():
    with test_context() as ctx:
        manifest = get_unparsed_manifest("jaffle_shop_metrics")

        response = ctx.client.post(
            "/push",
            json={"state_id": "abc-123", "install_deps": False, "body": manifest},
        )
        assert response.status_code == 200
        assert response.json() == {
            "bytes": 29,
            "path": "./working-dir/state-abc-123",
            "reuse": False,
            "state": "abc-123",
        }
