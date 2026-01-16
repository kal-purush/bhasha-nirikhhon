from django.contrib import admin
from .models import Post

# Register your models here.
admin.site.register(Post)

#@admin.register(Post)
#class PostAdmin(admin.ModelAdmin):
#    list_display = ('title', 'slug', 'author', 'date_posted', 'status')
#    list_filter = ('status', 'created', 'date_posted', 'author')
#    search_fields = ('title', 'body')
#    prepopulated_fields = {'slug': ('title',)}
#    raw_id_fields = ('author',)
#    date_hierarchy = 'date_posted'
#    ordering = ('status', 'date_posted')