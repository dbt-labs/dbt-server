from dbt_server.server import app
from fastapi.testclient import TestClient


client = TestClient(app)


def test_ready():
    response = client.post("/ready")
    assert response.status_code == 200
    assert response.json() == {}
