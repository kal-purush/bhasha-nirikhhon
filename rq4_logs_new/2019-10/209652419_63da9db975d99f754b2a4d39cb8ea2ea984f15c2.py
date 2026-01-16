# конвертирует список с p[(5,),(8,),...] к [5,8,...]
def _convert(list_convert):
    return [itm[0] for itm in list_convert]


# счетает общую сумму заказа и возвращает результат
def total_cost(list_quantity, list_price):
    order_total_cost = 0

    for ind, itm in enumerate(list_price):
        order_total_cost += list_quantity[ind] * list_price[ind]

    return order_total_cost


# счетает  общее количество заказанной единицы товара и возвращает результат
def total_quantity(list_quantity):
    order_total_quantity = 0

    for itm in list_quantity:
        order_total_quantity += itm

    return order_total_quantity


def get_total_cost(db):
    """ 
    Возвращает общую стоимость товара
    """
    # получаем список все product_id заказа
    all_product_id = db.select_all_product_id()
    # получаем список стоимость по всем позициям заказа в виде обычного списка
    all_price = [db.select_single_product_price(itm) for itm in all_product_id]
    # получаем список количества по всем позициям заказа в виде обычного списка
    all_quantity = [db.select_order_quantity(itm) for itm in all_product_id]
    # Возвращает общую стоимость товара
    return total_cost(all_quantity, all_price)


def get_total_quantity(db):
    """ 
    Возвращает общее количество заказанной единицы товара
    """
    # получаем список все product_id заказа
    all_product_id = db.select_all_product_id()
    # получаем список количества по всем позициям заказа в виде обычного списка
    all_quantity = [db.select_order_quantity(itm) for itm in all_product_id]
    # Возвращает общее количество заказанной единицы товара
    return total_quantity(all_quantity)