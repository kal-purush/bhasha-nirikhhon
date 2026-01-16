class Coffee:
    def __init__(self, name):
        self.name = name
        self._orders = [] 

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, str) and len(value) >= 3:
            self._name = value
        else:
            raise ValueError("Coffee name must be at least 3 characters long.")

    def orders(self):
        """Returns all orders associated with this coffee."""
        return self._orders

    def add_order(self, order):
        """Adds an order to the coffee."""
        self._orders.append(order)

    def customers(self):
        """Returns a unique list of customers who have ordered this coffee."""
        return list(set(order.customer for order in self._orders))

    def num_orders(self):
        """Returns the total number of orders for this coffee."""
        return len(self._orders)

    def average_price(self):
        """Returns the average price of all orders for this coffee."""
        if len(self._orders) == 0:
            return 0
        return sum(order.price for order in self._orders) / len(self._orders)
    def __repr__(self):
        return f"Coffee({self.name})"