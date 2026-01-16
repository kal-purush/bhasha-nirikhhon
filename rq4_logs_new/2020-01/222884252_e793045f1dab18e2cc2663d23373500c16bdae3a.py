from django.contrib import admin
from .models import Table

# Register your models here.

@admin.register(Table)
class Table(admin.ModelAdmin):
    pass