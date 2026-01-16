import pytest
from django.urls import reverse

from list.models import ShoppingList


@pytest.mark.django_db
def test_get_list_response(client, shopping_list):
    response = client.get(reverse('api:shopping-list-list'))

    assert response.status_code == 200
    assert len(response.data) == ShoppingList.objects.count()


@pytest.mark.django_db
def test_get_detail_response(client, shopping_list):
    response = client.get(reverse('api:shopping-list-detail', kwargs={'pk': shopping_list.pk}))
    assert response.status_code == 200

    data = response.json()
    assert data['name'] == shopping_list.name
    assert data['created_by'] == shopping_list.created_by.pk


def test_post_response(client, admin_user):
    response = client.post(
        path=reverse('api:shopping-list-list'),
        data={'name': 'test_list', 'created_by': admin_user.pk}
    )

    assert response.status_code == 201
    assert ShoppingList.objects.get(name='test_list')