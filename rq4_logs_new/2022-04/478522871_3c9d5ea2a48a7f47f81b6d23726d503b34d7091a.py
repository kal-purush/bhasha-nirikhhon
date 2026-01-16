from django.contrib import admin
from .models import *

# Register your models here.
class CustomerAdmi(admin.ModelAdmin):
    list_display = ['name','phone','user','email']
    search_fields = ['name','date_created']
    list_per_page = 5
admin.site.register(Customer,CustomerAdmi)

class ProductAdmin(admin.ModelAdmin):
    list_display = ['name', 'price', 'category','date_created','description']
    search_fields = ['name', 'price', 'category','date_created','description']
    list_per_page = 5
admin.site.register(Product,ProductAdmin)

class OrderAdmin(admin.ModelAdmin  ):
    list_display = ['customer', 'product', 'status','date_created']
    search_fields = ['customer', 'product', 'status','date_created']
    list_per_page = 5
admin.site.register(Order,OrderAdmin)
