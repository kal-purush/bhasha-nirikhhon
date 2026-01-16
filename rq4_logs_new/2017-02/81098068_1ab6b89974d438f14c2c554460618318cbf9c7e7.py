from django.contrib import admin

# Register your models here.
from transportapp.models import Transpor_Type,Route,Traffic_Stop,Reasearcher,Reasearch,Reasearch_detail,Season,Day_Type,Time_of_Day,Result

admin.site.register(Transpor_Type)
admin.site.register(Traffic_Stop)
admin.site.register(Reasearch)
admin.site.register(Reasearcher)
admin.site.register(Reasearch_detail)
admin.site.register(Season)
admin.site.register(Time_of_Day)
admin.site.register(Day_Type)
admin.site.register(Result)