from django.contrib import admin
from .models import App, OrganizationalUnitType, OrganizationalUnit

admin.site.register(OrganizationalUnitType)
admin.site.register(OrganizationalUnit)
admin.site.register(App)