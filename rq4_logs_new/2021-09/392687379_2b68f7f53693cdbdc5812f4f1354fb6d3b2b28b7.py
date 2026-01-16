from django.urls import path, include

from .views import (
    generate_student,
    create_student_form,
    view_students,
    edit_student,
    delete_student
)

urlpatterns = [
    path('generate/<int:count>', generate_student, name='generate-students'),
    path('generate/', generate_student, name='generate-student'),
    path('create/', create_student_form, name='create-student'),
    path('view/', view_students, name='view-students'),
    path('edit<int:student_id>', edit_student, name='edit-student'),
    path('delete<int:student_id>', delete_student, name='delete-student'),
]