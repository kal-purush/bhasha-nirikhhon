from django.contrib import admin
from .models import Post
# Register your models here.

class PostAdmin(admin.ModelAdmin):
    class Media:
        css = {
            "all":("css/main.css",)
        }
        js = ("js/apps.js",)
    list_display = ['id', 'title', 'status', 'publish', 'author' ]
admin.site.register(Post, PostAdmin)