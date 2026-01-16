class BaseProduct:
    def __init__(self, product: str, price: float, amount=1):
        self.product_name = product
        self.price = price
        self.amount = amount

    def total_price(self):
        result = self.price * self.amount

        return result


class Water250ml(BaseProduct):
    pass


class Coffee(BaseProduct):
    pass


class Lemonade(BaseProduct):
    pass


class OpenBill:
    def __init__(self, *products):
        self.products = products

    def total_price(self):
        result = 0

        for product in self.products:
            result += product.total_price()

        return result

    def list_the_products(self):
        result_list = []

        for product in self.products:
            result_list.append(f"Product: {product.product_name}, Amount: {product.amount}, "
                               f"Price: {product.total_price()} lv.")

        result = "\n".join(result_list)

        return result


water = Water250ml("Devin", 2.00, 2)
coffee = Coffee("Dabov", 3.00, 4)

bill = OpenBill(water, coffee)

print(bill.list_the_products())
print("Total:")
print(bill.total_price())