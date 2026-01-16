from django.contrib import admin
from .models import User, Employee, Project
from django.contrib.auth.models import Group
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin


# Customizing the User admin interface
@admin.register(User)
class UserAdmin(BaseUserAdmin):
    model = User
    list_display = ('username', 'email', 'is_staff', 'is_superuser')
    list_filter = ('is_staff', 'is_superuser', 'groups')
    search_fields = ('username', 'email')
    ordering = ('username',)

    fieldsets = (
        ('Change User', {'fields': ('username', 'email', 'password')}),
        ('Personal Info', {'fields': ('first_name', 'last_name')}),
        ('Permissions', {'fields': ('is_staff', 'is_superuser', 'groups', 'user_permissions')}),
        ('Important dates', {'fields': ('last_login', 'date_joined')}),
    )

    add_fieldsets = (
        ('Add User', {'fields': ('username', 'email', 'password1', 'password2', 'is_staff', 'is_superuser', 'groups', 'user_permissions'),}),
    )


# Customizing the Employee admin interface
@admin.register(Employee)
class EmployeeAdmin(admin.ModelAdmin):
    list_display = ('name', 'email', 'domain', 'phonenumber', 'experience', 'company', 'dateofjoining')
    search_fields = ('name', 'email', 'domain')
    
    fieldsets = [
        ("Personal Info", {"fields": ["name", "email", "dob", "phonenumber"]}),
        ("Professional Info", {"fields": ["domain", "company", 'dateofjoining', "experience"]}),
    ]

# Customizing the Project admin interface
@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ('projectname', 'employee', 'status', 'startdate', 'enddate')
    list_filter = ('status',)
    search_fields = ('projectname', 'employee__name')
    
    fieldsets = [
        ("Project Details", {"fields": ["projectname", "employee", "status"]}),
        ("Schedule", {"fields": ["startdate", "enddate"]}),
    ]