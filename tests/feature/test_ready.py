def test_ready(test_client):
    response = test_client.post("/ready")
    assert response.status_code == 200
    assert response.json() == {}
