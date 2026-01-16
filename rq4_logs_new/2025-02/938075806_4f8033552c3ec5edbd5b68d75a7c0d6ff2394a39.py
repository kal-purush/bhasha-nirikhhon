from django.contrib import admin
from account.models import User

# admin.site.register(User)
@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ("id","first_name","last_name","email")
    list_display_links = ("id","first_name","last_name")
    list_filter = ("is_staff","is_superuser","is_active")
    search_fields = ("email","first_name","last_name")
    readonly_fields = ("last_login","created_at","updated_at")