from django.contrib import admin
from .models import Assignment, Question, Choice
# Register your models here.


class ChoiceInLine(admin.TabularInline):
    model = Choice


class QuestionInline(admin.TabularInline):
    model = Question


class QuestionAdmin(admin.ModelAdmin):
    # radio_fields = {"assignment": admin.VERTICAL}
    raw_id_fields = ("assignment",)
    list_display = ('assignment', 'question_text', 'id')
    list_display_links = ('question_text', 'id')
    list_filter = ('question_text', 'assignment')
    inlines = [ChoiceInLine]
    search_fields = ['question_text', 'id']


class AssignmentAdmin(admin.ModelAdmin):

    list_display = ('subject', 'id', 'class_name')
    list_display_links = ('subject', 'id')
    list_filter = ('subject', 'class_name')
    search_fields = ['subject', 'id']


admin.site.register(Assignment, AssignmentAdmin)
admin.site.register(Question, QuestionAdmin)
admin.site.register(Choice)