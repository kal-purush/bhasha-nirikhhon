import factory
from datetime import datetime
from housesales.models import HouseSales


class HouseSalesFactory(factory.DjangoModelFactory):
    FACTORY_FOR = HouseSales

    transaction_id = "1A75C2AB-A2DF-4549-A938-00008A73F7CF"
    price = 250000
    date_of_transfer = datetime.date(2015, 6, 13)
    postcode = "SE13 7AW"
    property_type = "F"
    old_new = "Y"
    duration = "F"
    paon = "paon"
    saon = "saon"
    street = "London Road"
    locality = "London"
    town_city = "London"
    district = "Tower Hamlets"
    county = "Bow"
    status = "A"