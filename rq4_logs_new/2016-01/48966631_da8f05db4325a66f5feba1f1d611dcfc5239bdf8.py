import os
import sys
import pytest
from mock import Mock, patch

app_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, app_path + '/../')

from orders import Book, Buy, Sell

class TestBook(object):

    def test_book_instance(self):
        """
        Test that Book() is a singleton.
        """
        book1 = Book()
        book2 = Book()
        assert id(book1) == id(book2)

    def test_ismatch_buy(self):
        """
        Test that order=Buy matches entry=Sell.
        """
        order = Buy(qty=1, prc=1)
        entry = Sell(qty=1, prc=1)
        flag = Book()._ismatch(order, entry)
        assert flag is True

    def test_ismatch_sell(self):
        """
        Test that order=Sell matches entry=Buy.
        """
        order = Sell(qty=1, prc=1)
        entry = Buy(qty=1, prc=1)
        flag = Book()._ismatch(order, entry)
        assert flag is True

    def test_post_order_buy(self):
        """
        Test post order with buy.
        """
        order = Buy(qty=1, prc=1)
        entry = Sell(qty=1, prc=1)
        flag = Book()._post(order, entry)
        assert flag is True

    def test_post_order_sell(self):
        """
        Test post order with sell.
        """
        order = Buy(qty=1, prc=1)
        entry = Sell(qty=1, prc=1)
        flag = Book()._post(order, entry)
        assert flag is True

    def test_match_buy_against_complete_matching_sells(self):
        """
        Test buy against complete matching sell.
        """
        buy = Buy(qty=1, prc=1)
        sell1 = Sell(qty=1, prc=1)
        sell2 = Sell(qty=1, prc=2)
        fills1 = Book().match(sell1)
        fills2 = Book().match(sell2)
        fills3 = Book().match(buy)
        orders = Book().orders()
        expected = {"sells": [{"qty": 1, "prc": 2}], "buys": [],}
        assert fills1 == []
        assert fills2 == []
        assert fills3 == [{"qty": 1, "prc": 1,}]
        assert orders == expected

    def test_match_sell_against_partial_matching_buy(self):
        """
        Test sell against partial matching buys.
        """
        Book()._clear()
        sell = Sell(qty=1, prc=2)
        buy1 = Buy(qty=1, prc=1)
        buy2 = Buy(qty=2, prc=2)
        fills1 = Book().match(buy1)
        fills2 = Book().match(buy2)
        fills3 = Book().match(sell)
        orders = Book().orders()
        expected = {"sells": [], "buys": [{"qty": 1, "prc": 2},
                                          {"qty": 1, "prc": 1}],}
        assert fills1 == []
        assert fills2 == []
        assert fills3 == [{"qty": 1, "prc": 2,}]
        assert orders == expected

    def test_match_sell_against_short_matching_buy(self):
        """
        Test sell against short matching buy.
        """
        Book()._clear()
        sell = Sell(qty=2, prc=2)
        buy1 = Buy(qty=1, prc=1)
        buy2 = Buy(qty=1, prc=2)
        fills1 = Book().match(buy1)
        fills2 = Book().match(buy2)
        fills3 = Book().match(sell)
        orders = Book().orders()
        expected = {"sells": [{"qty": 1, "prc": 2},],
                    "buys": [{"qty": 1, "prc": 1},],}
        assert fills1 == []
        assert fills2 == []
        assert fills3 == [{"qty": 1, "prc": 2,},]
        assert orders == expected

    def test_match_sell_against_partial_short_matching_buy(self):
        """
        Test sell against multiple short matching buys.
        """
        Book()._clear()
        sell = Sell(qty=2, prc=2)
        buy1 = Buy(qty=1, prc=1)
        buy2 = Buy(qty=1, prc=2)
        buy3 = Buy(qty=1, prc=3)
        fills1 = Book().match(buy1)
        fills2 = Book().match(buy2)
        fills3 = Book().match(buy3)
        fills4 = Book().match(sell)
        orders = Book().orders()
        expected = {"sells": [],
                    "buys": [{"qty": 1, "prc": 1},],}
        assert fills1 == []
        assert fills2 == []
        assert fills3 == []
        assert fills4 == [{"qty": 1, "prc": 3}, {"qty": 1, "prc": 2,},]
        assert orders == expected

    def test_orders(self):
        """
        Test that un-matched orders return properly.
        """
        Book()._clear()
        sell = Sell(qty=1, prc=3)
        buy = Buy(qty=1, prc=2)
        fills1 = Book().match(sell)
        fills2 = Book().match(buy)
        orders = Book().orders()
        expected = {"sells": [{"qty": 1, "prc": 3},],
                    "buys": [{"qty": 1, "prc": 2},],}
        assert fills1 == []
        assert fills2 == []
        assert orders == expected


class TestOrderMethods(object):

    def test_order_sub(self):
        """
        Test __sub__ magic method on Order objects.
        """
        buy = Buy(qty=1, prc=1)
        sell = Sell(qty=2, prc=1)
        diff = sell - buy
        expected = {"qty": 1, "prc": 1}
        assert diff == expected

    def test_order_sub_short(self):
        """
        Test __sub__ magic method on Order objects.
        """
        buy = Buy(qty=2, prc=1)
        sell = Sell(qty=1, prc=1)
        with pytest.raises(Exception) as excinfo:
            diff = sell - buy
        assert excinfo.value[0] == "Short of shares to fill."