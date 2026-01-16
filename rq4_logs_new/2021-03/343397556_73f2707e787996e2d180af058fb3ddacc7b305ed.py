from django.contrib import admin
from import_export.admin import ImportExportModelAdmin

from . models import Convenor,Coordinator,HeadCoordinator,Manager
# Register your models here.
# admin.site.register(Convenor)
# admin.site.register(Coordinator)
# admin.site.register(HeadCoordinator)
# admin.site.register(Manager)


@admin.register(Convenor)
class ConvenorAdmin(ImportExportModelAdmin):
    pass

@admin.register(Coordinator)
class ContactAdmin(ImportExportModelAdmin):
    pass


@admin.register(HeadCoordinator)
class HeadCoordinatorAdmin(ImportExportModelAdmin):
    pass


@admin.register(Manager)
class ManagerAdmin(ImportExportModelAdmin):
    pass