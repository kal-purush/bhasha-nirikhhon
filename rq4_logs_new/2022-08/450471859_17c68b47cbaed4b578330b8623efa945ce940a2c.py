import requests

api_endpoint = "api/search_service"
test_headers = {"Content-Type": "application/json", "Accept": "application/json"}
test_data_store = {}


def test_namespaced_json_resource_create(http_service):
    """ """
    namespaces = ["urn:test:value:1"]
    test_endpoint = "json_resource"
    status = 201
    post_json = {
        "label": "A Test Resource",
        "data": {"key_1": "Value 1", "key_2": "Value 2"},
        "namespaces": namespaces,
    }
    response = requests.post(
        f"{http_service}/{api_endpoint}/{test_endpoint}/",
        json=post_json,
        headers=test_headers,
    )
    response_json = response.json()
    assert response.status_code == status
    assert response_json.get("label") == post_json.get("label")
    assert response_json.get("data") == post_json.get("data")
    assert response_json.get("created") is not None
    assert response_json.get("modified") is not None
    assert response_json.get("id") is not None
    assert response_json.get("namespaces") == namespaces
    test_data_store["json_resource_id"] = response_json.get("id")


def test_namespace_created(http_service):
    """ """
    namespaces = ["urn:test:value:1"]
    test_endpoint = "namespace"
    status = 200
    for ns_urn in namespaces:
        response = requests.get(
            f"{http_service}/{api_endpoint}/{test_endpoint}/{ns_urn}",
            headers=test_headers,
        )
        response_json = response.json()
        assert response.status_code == status
        assert response_json.get("urn") == ns_urn


def test_namespaced_json_resource_indexables_creation(http_service):
    namespaces = ["urn:test:value:1"]
    test_endpoint = "indexable"
    status = 200
    resource_id = test_data_store.get("json_resource_id")
    response = requests.get(
        f"{http_service}/{api_endpoint}/{test_endpoint}/", headers=test_headers
    )
    response_json = response.json()
    assert response.status_code == status
    assert len(response_json.get("results")) == 3
    for indexable in response_json.get("results"):
        assert indexable.get("resource_id") == resource_id
        assert indexable.get("namespaces") == namespaces

def test_namespaced_json_resource_cleanup(http_service):
    # Get a list of all resources
    test_endpoint = "json_resource"
    status = 200
    response = requests.get(
        f"{http_service}/{api_endpoint}/{test_endpoint}/",
        headers=test_headers,
    )
    response_json = response.json()
    assert response.status_code == status
    # Delete all resources
    for resource_id in [res.get("id") for res in response_json.get("results")]:
        test_endpoint = f"json_resource/{resource_id}"
        status = 204
        response = requests.delete(
            f"{http_service}/{api_endpoint}/{test_endpoint}/", headers=test_headers
        )
        assert response.status_code == status
    # Check indexables have also been deleted
    test_endpoint = "indexable"
    status = 200
    response = requests.get(
        f"{http_service}/{api_endpoint}/{test_endpoint}/", headers=test_headers
    )
    response_json = response.json()
    assert response.status_code == status
    assert len(response_json.get("results")) == 0