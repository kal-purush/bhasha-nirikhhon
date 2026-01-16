"""
Test Factory to make fake objects for testing
"""
from datetime import date
import factory
from factory.fuzzy import FuzzyInteger, FuzzyDate
from service.models import Wishlist


class WishlistFactory(factory.Factory):
    """Creates fake Wishlists"""

    # pylint: disable=too-few-public-methods
    class Meta:
        """Persistent class"""
        model = Wishlist

    wishlist_id = factory.Sequence(lambda n: n)
    customer_id = FuzzyInteger(0, 255) # random customer id
    date_joined = FuzzyDate(date(2008, 1, 1)) # random date from _ to today
    