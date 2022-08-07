from tests.feature.context import test_context


def test_ready():
    with test_context() as ctx:
        response = ctx.client.post("/ready")
        assert response.status_code == 200
        assert response.json() == {}
