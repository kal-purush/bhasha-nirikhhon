# import shutil
# import tempfile
# from django.conf import settings
# from django.conf.global_settings import MEDIA_ROOT

# from django.test import TestCase, override_settings
# from django.contrib.auth import get_user_model
# from django.core.files.uploadedfile import SimpleUploadedFile
# from rest_framework.test import APIClient

# from products.models import (
#     Category,
#     Brand,
#     CountryProduct,
#     Product,
#     Shop_basket,
#     Shop_basket_items,
#     Order,
#     OrderItems,)


# USER = get_user_model()

# TEMP_MEDIA_ROOT = tempfile.mkdtemp(dir=settings.BASE_DIR)


# @override_settings(MEDIA_ROOT=TEMP_MEDIA_ROOT)
# class ProductsViewsTests(TestCase):
#     ...