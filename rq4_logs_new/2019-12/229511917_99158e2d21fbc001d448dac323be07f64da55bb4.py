#from django.contrib import admin

# Register your models here.
from django.contrib.admin import AdminSite
from .models import Product, Attribute, Location, Wheel

class ProductManagerAdminSite(AdminSite):
    site_header = "Product Manager"
    site_title = "Product Manager Portal"
    index_title = "Butler Tire - Product Manager"

product_admin_site = ProductManagerAdminSite(name='product_manager')

## Register Each Model
product_admin_site.register(Product)
product_admin_site.register(Attribute)
product_admin_site.register(Location)
product_admin_site.register(Wheel)