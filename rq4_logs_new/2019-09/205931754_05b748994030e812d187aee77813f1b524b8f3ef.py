from django.contrib.auth import get_user_model
from django.urls import reverse


def test_get_list_response(api_client):
    response = api_client.get(reverse('api:user-list'))

    assert response.status_code == 200
    assert len(response.data) == get_user_model().objects.count()


def test_get_detail_response(api_client, admin_user):
    response = api_client.get(reverse('api:user-detail', kwargs={'pk': admin_user.pk}))
    assert response.status_code == 200

    data = response.json()
    assert data['id'] == admin_user.id
    assert data['username'] == admin_user.username
    assert data['first_name'] == admin_user.first_name
    assert data['last_name'] == admin_user.last_name


def test_cannot_create(api_client):
    response = api_client.post(
        path=reverse('api:user-list'),
        data={
            'username': 'm.ozil',
            'first_name': 'mesut',
            'last_name': 'ozil',
        }
    )

    assert response.status_code == 405


def test_cannot_edit(api_client, admin_user):
    put = api_client.put(
        path=f'{reverse("api:user-list")}{admin_user.pk}/',
        data={
            'name': 'Mesut',
        }
    )

    patch = api_client.patch(
        path=f'{reverse("api:user-list")}{admin_user.pk}/',
        data={
            'name': 'Mesut',
        }
    )

    assert put.status_code == 405
    assert patch.status_code == 405


def test_cannot_delete(api_client, admin_user):
    response = api_client.delete(
        path=f'{reverse("api:user-list")}{admin_user.pk}/'
    )

    assert response.status_code == 405