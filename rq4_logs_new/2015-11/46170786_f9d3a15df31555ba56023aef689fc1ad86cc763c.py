from django.contrib import admin
from bookapp.models import *

class BookAdmin(admin.ModelAdmin):
    pass

admin.site.register(Book)
admin.site.register(Author)