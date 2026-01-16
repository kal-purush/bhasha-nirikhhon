from django.contrib import admin

# Register your models here.
from example.models import Company, CompanyGroup

admin.site.register(Company)
admin.site.register(CompanyGroup)