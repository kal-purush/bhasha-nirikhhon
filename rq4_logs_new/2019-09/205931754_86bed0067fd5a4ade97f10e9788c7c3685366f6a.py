from django.urls import reverse

from list.models import Item


def test_get_list_response(client):
    response = client.get(reverse('api:item-list'))

    assert response.status_code == 200
    assert len(response.data) == Item.objects.count()


def test_get_detail_response(client, item_banana):
    response = client.get(reverse('api:item-detail', kwargs={'pk': item_banana.pk}))
    assert response.status_code == 200

    data = response.json()
    assert data['name'] == item_banana.name
    assert data['quantity'] == item_banana.quantity
    assert data['added_by'] == item_banana.added_by.pk


def test_post_response(client, admin_user, shopping_list):
    response = client.post(
        path=reverse('api:item-list'),
        data={
            'name': 'onion',
            'quantity': '200kg',
            'list': shopping_list.pk,
            'added_by': admin_user.pk
        }
    )

    assert response.status_code == 201
    item = Item.objects.get(name='onion')
    assert item.name == 'onion'
    assert item.quantity == '200kg'
    assert item.list == shopping_list
    assert item.added_by == admin_user