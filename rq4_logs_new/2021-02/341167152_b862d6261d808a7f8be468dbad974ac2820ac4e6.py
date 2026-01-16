from django.db import models
from django.contrib.auth.models import User

GEAR_BOX_TYPE = (
    ('manual', 'Manual'),
    ('automatic', 'Automatic'),
)

NEW_OR_USED = (
    ('new', 'New'),
    ('used', 'Used'),
)

BODY_TYPE = (
    ('convertible', 'Convertible'),
    ('coupe', 'Coupe'),
    ('minivan', 'Minivan'),
    ('sedan', 'Sedan'),
    ('suv', 'Suv'),
    ('wagon', 'Wagon'),
)

FUEL_TYPE = (
    ('diesel', 'Diesel'),
    ('Gasoline', 'Gasoline')
)


class Brand(models.Model):
    name = models.CharField(max_length=255, db_index=True)
    country_of_origin = models.CharField(max_length=255, default=None)

    def __str__(self):
        return self.name


class CarModel(models.Model):
    brand_name = models.ForeignKey(Brand, on_delete=models.CASCADE)
    name = models.CharField(max_length=255, db_index=True)

    def __str__(self):
        return self.name


class Product(models.Model):
    brand_name = models.ForeignKey(Brand, on_delete=models.CASCADE)
    car_model = models.ForeignKey(CarModel, on_delete=models.CASCADE)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='product_creator')
    title = models.CharField(max_length=255)
    year_of_production = models.IntegerField(default=0)
    engine = models.CharField(max_length=50)
    gear_box = models.CharField(choices=GEAR_BOX_TYPE, max_length=30, default='')
    new_or_used = models.CharField(choices=NEW_OR_USED, max_length=30, default='')
    body_type = models.CharField(choices=BODY_TYPE, max_length=30, default='')
    fuel_type = models.CharField(choices=FUEL_TYPE, max_length=30, default='')
    color = models.CharField(max_length=50)
    description = models.TextField(blank=True)
    image = models.ImageField(upload_to='images/', blank=True)
    price = models.DecimalField(max_digits=7, decimal_places=2)
    in_stock = models.BooleanField(default=True)
    is_active = models.BooleanField(default=True)
    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.title