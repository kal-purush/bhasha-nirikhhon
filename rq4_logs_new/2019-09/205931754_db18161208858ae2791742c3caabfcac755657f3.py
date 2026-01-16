import pytest

from list.models import Item, ShoppingList


@pytest.fixture
def shopping_list(admin_user):
    return ShoppingList.objects.create(
        name='food',
        created_by=admin_user,
    )


def test_can_create(shopping_list, admin_user):
    item = Item.objects.create(
        name='onions',
        quantity='5',
        list=shopping_list,
        added_by=admin_user,
    )

    assert item.name == 'onions'
    assert item.quantity == '5'
    assert item.list == shopping_list
    assert item.added_by == admin_user


def test_model_str(shopping_list, admin_user):
    item = Item.objects.create(
        name='onions',
        quantity='5',
        list=shopping_list,
        added_by=admin_user,
    )

    assert str(item) == 'onions'