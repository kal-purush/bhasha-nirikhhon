import shutil
import tempfile

from django.test import TestCase, override_settings
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile

from products.models import (
    Category,
    Brand,
    CountryProduct,
    Product,
    Shop_basket,
    Shop_basket_items,
    Order,
    OrderItems,)


MEDIA_ROOT = tempfile.mkdtemp()


@override_settings(MEDIA_ROOT=MEDIA_ROOT)
class CategoryModelTest(TestCase):
    """
    Тестирование модели категорий.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.category = Category.objects.create(
            name='Тестовая категория',
            description='Тестовое описание',
            image=SimpleUploadedFile(
                'test.jpg', b'something'),
            slug='test_category',
            meta_title='test_meta_title',
            meta_description='test_meta_description'
        )

    def test_model_have_correct_object_name(self):
        """
        Тестирование корректности отображения
        __str__ у модели.
        """

        category = CategoryModelTest.category
        print(f'CATEGORY {category}')
        expected_category_name = category.name
        print(f'EXPECTED_CATEGORY_NAME {expected_category_name}')
        self.assertEqual(expected_category_name, str(category))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()