from django.contrib import admin


# Register your models here.
from .models import Monedas
from .models import Paridades

admin.site.register(Monedas)
admin.site.register(Paridades)