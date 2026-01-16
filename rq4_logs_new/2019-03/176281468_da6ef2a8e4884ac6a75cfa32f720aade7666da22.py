from django.contrib import admin
from .models import Member, Question, Answer, Response
from django.contrib.auth.models import Group

class AnswerInline(admin.StackedInline):
    model = Answer
    extra = 4

@admin.register(Question)
class QuestionAdmin(admin.ModelAdmin):
    fields = ['questionkey', 'content', 'score_increment', 'score_decrement', 'image', ('is_image', 'is_mcq')]
    inlines = [AnswerInline]


admin.autodiscover()
admin.site.register(Member)
# admin.site.register(Question)
admin.site.register(Answer)
admin.site.register(Response)