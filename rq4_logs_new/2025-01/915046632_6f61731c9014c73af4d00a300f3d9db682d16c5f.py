# admin.py
from django.contrib import admin
from .models import Coffee, Tea, Pastry, HeroImage

class CoffeeAdmin(admin.ModelAdmin):
    list_display = ('name', 'price', 'quantity', 'category', 'image')

class TeaAdmin(admin.ModelAdmin):
    list_display = ('name', 'price', 'quantity', 'category', 'image')

class PastryAdmin(admin.ModelAdmin):
    list_display = ('name', 'price', 'quantity', 'category', 'image')

class HeroImageAdmin(admin.ModelAdmin):
    list_display = ('title', 'is_active', 'image')  # Show useful fields in the list view
    search_fields = ('title', 'subtitle')  # Allow search by title and subtitle

admin.site.register(Coffee, CoffeeAdmin)
admin.site.register(Tea, TeaAdmin)
admin.site.register(Pastry, PastryAdmin)
admin.site.register(HeroImage, HeroImageAdmin)