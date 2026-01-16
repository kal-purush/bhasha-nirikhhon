from django.contrib import admin
from .models import Usvers, Products

# Register your models here.

#class UsversAdmin(admin.ModelAdmin):
#    list_display = ('USERNAME', 'EMAIL ADDRESS', 'FIRST NAME', 'LAST NAME', 'STAFF STATUS', 'balance')
#    #fields = ['first_name', 'last_name', ('date_of_birth', 'date_of_death')]
class UsversAdmin(admin.ModelAdmin):
    list_display = ['user', 'balance', 'phone']


admin.site.register(Usvers, UsversAdmin)
admin.site.register(Products)