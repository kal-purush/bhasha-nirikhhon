from django.contrib import admin
# Register your models here.
from ride.models import Ride


class RideAdmin(admin.ModelAdmin):
    list_display = ("datetime_from", "datetime_to",)


admin.site.register(Ride, RideAdmin)