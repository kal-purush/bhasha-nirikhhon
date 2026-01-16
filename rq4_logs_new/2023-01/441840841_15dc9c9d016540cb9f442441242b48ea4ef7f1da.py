
from sqlalchemy import create_engine

import settings
from crawler.super.models import Base
from crawler.super.models import SuperProductDetails

class TestSuperModels(object):

    def fixture(cls):
        engine = create_engine(settings.DB_TEST_URL_SYNC)
        Base.metadata.create_all(bind=engine)
        SuperProductDetails.dsn = settings.DB_TEST_URL_SYNC
        yield
        Base.metadata.drop_all(bind=engine)


class TestUpsertMany(TestSuperModels):

    def test_upsert_many(self):
        l = [
            SuperProductDetails(jan="4444", price=2000, set_number=1, shop_code="test", product_code="test"),
            SuperProductDetails(jan="4444", price=1000, set_number=1, shop_code="test", product_code="test2"),
        ]

        result = SuperProductDetails.upsert_many(l)

        assert result == True