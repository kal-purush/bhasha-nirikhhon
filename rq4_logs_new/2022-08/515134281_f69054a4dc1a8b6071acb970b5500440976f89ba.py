from atexit import register
from django.contrib import admin
from .models import question_model, answer_model, user_model


@admin.register(user_model)
class userModelAdmin(admin.ModelAdmin):
    list_display = ['auto_user_id', 'full_name', 'nick_name', 'date_of_birth', 'contact_number', 'about', 'company_name', 'designation', 'rating_name']
    

@admin.register(question_model)
class questionModelAdmin(admin.ModelAdmin):
    list_display = ['auto_question_id', 'question_title', 'question_description', 'technology_name', 'status_name', 'created_at', 'get_questions']
            

@admin.register(answer_model)
class answerModelAdmin(admin.ModelAdmin):
    list_display = ['auto_answer_id', 'created_at', 'like_and_dislike', 'technology_name', 'status_name', 'user_answer', 'get_answers']

    