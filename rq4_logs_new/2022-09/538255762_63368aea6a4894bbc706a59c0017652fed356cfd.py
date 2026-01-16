# Database.py - function for Managing Database

import sqlite3

connection = sqlite3.connect("shopping_list.db")

def get_items(id=None):
    cursor = connection.cursor()
    if id:
        items = cursor.execute(f"select id,description from list where id= {id}")
    else:
        items = cursor.execute("select id,description from list")
    items = list(items)
    items = [{"id":item[0],"desc":item[1]} for item in items]
    return items

def test_get_items():
    print("testing get items")
    items = get_items()
    assert type(items) is list
    assert len(items) > 0
    assert type(items[0]) is dict
    assert 'id' in items[0].keys()
    assert 'desc' in items[0].keys()
    assert type(items[0]['id']) is int
    assert type(items[0]['desc']) is str
    pass

def add_items(description):
    cursor = connection.cursor()
    cursor.execute(f"insert into list(description) values('{description}')")
    connection.commit()

import time

def random_string():
    return str(time.time())


def test_add_items():
    print("Testing Add items")
    description = random_string()
    add_items(description)
    items = get_items()
    item = items[-1]
    assert description == item['desc']


def delete_items(id):
    cursor = connection.cursor()
    cursor.execute(f"delete from list where id = {id}")
    connection.commit()

def test_delete_items():
    print("testing delete items")
    description = random_string()
    add_items(description)
    items = get_items()
    item = items[-1]
    assert description == item['desc']
    delete_items(item['id'])
    items = get_items()
    for item in items:
        assert description != item['desc']


def update_items(id,description):
    cursor = connection.cursor()
    cursor.execute(f"update list set description = '{description}' where id ={id}")
    connection.commit()

def test_update_items():
    print("testing Update items")
    description = random_string()
    add_items(description)
    items = get_items()
    item = items[-1]
    id = str(item['id'])
    description = item['desc']
    new_desc = description.replace("1","9").replace(".",":")
    update_items(id,new_desc)
    items = get_items()
    new_found = False
    for item in items:
        if item['id'] == int(id):
            assert item['desc'] == new_desc
            new_found = True
        assert item['desc'] != description
    assert new_found



#Now to test the above functions externally , the below module runs directly instead of being run as library

if  __name__ == "__main__":
    test_get_items()
    test_add_items()
    test_delete_items()
    test_update_items()
    print("done")