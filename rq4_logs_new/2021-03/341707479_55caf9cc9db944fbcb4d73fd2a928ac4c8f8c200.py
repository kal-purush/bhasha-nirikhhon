

def calculate_total_price(products):

    total_price = 0

    for product in products:
        total_price += product.price

    return total_price