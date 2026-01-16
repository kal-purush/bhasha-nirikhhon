# accounts/admin.py
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import User

# class UserAdmin(BaseUserAdmin):
#     # Define the forms to add and change user instances
#     add_form = None  # You may want to create and add forms for add and change operations
    
#     form = None  # You may want to create and add forms for add and change operations
#     model = User
    
#     list_display = ('email', 'first_name', 'last_name', 'country', 'zip_code', 'is_staff', 'is_active')
#     list_filter = ('is_staff', 'is_active', 'country')
#     search_fields = ('email', 'first_name', 'last_name')
#     ordering = ('email',)
#     readonly_fields = ('is_active', 'is_staff')

#     fieldsets = (
#         (None, {'fields': ('email', 'first_name', 'last_name', 'country', 'zip_code', 'password')}),
#         ('Permissions', {'fields': ('is_active', 'is_staff', 'is_superuser', 'user_permissions', 'groups')}),
#         ('Important Dates', {'fields': ('last_login', 'date_joined')}),
#     )

#     add_fieldsets = (
#         (None, {
#             'classes': ('wide',),
#             'fields': ('email', 'first_name', 'last_name', 'country', 'zip_code', 'password1', 'password2'),
#         }),
#     )

admin.site.register(User)