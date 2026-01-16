from django.contrib import admin
from .models import Volunteer,Executive,Cause,Event,JoinEvent

class CauseAdmin(admin.ModelAdmin):
    list_display = ("cause_title",)
    prepopulated_fields = {"slug": ("cause_title",)}  
    
class EventAdmin(admin.ModelAdmin):
    list_display = ("event_title",)
    prepopulated_fields = {"slug": ("event_title",)}  

admin.site.register(Volunteer)
admin.site.register(Executive)
admin.site.register(Cause,CauseAdmin)
admin.site.register(Event,EventAdmin)
admin.site.register(JoinEvent)