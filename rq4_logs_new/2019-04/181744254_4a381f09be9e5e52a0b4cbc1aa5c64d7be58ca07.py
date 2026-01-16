from django.contrib import admin
from django.contrib.contenttypes.admin import GenericStackedInline
from .models import FieldOverride

class FieldOverrideStackedInline(GenericStackedInline):
    model = FieldOverride
    extra = 0