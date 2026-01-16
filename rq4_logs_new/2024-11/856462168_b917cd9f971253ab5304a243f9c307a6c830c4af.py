from django.contrib import admin

# Register your models here.

from .models import Book, ReadingEntry, Review
admin.site.register(Book)
admin.site.register(ReadingEntry)
admin.site.register(Review)