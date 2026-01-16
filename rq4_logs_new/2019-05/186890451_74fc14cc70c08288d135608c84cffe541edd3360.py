from django.contrib import admin

from .models import Producer, CarModel, Car

admin.site.register(Producer)
admin.site.register(CarModel)
admin.site.register(Car)