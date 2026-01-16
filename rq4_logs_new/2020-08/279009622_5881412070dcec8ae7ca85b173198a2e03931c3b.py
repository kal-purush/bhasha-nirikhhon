import factory
from core.factories.product_line import ProductLineFactory

class ProductFactory (factory.DjangoModelFactory):
    class Meta:
        model = 'core.Product'
        django_get_or_create = ('title', 'product_line')

    product_line = factory.SubFactory(ProductLineFactory)
    title = factory.Faker('text', max_nb_chars=100)
    description = factory.Faker('text', max_nb_chars=500)
    price = factory.Faker('random_int')
    list_price = factory.Faker('random_int')
    quantity = factory.Faker('random_element', elements=(1, 5, 10, 20))
    inventory = factory.Faker('random_int')