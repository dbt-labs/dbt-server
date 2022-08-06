def test_push_parse_flow(test_client):
    """
    Test that the expected flow for getting a project setup in dbt-server works.
    Push, install dependencies, and parse.
    """

    response = test_client.post("/ready")
    assert response.status_code == 200
    assert response.json() == {}
