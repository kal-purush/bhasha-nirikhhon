from django.urls import path

from courses import views as courses_views
from django.conf.urls.static import static
from django.conf import settings


urlpatterns = [
    path('', courses_views.index, name='index'),
    path('', courses_views.index1.as_view(), name='home'),
    path('api/courses/', courses_views.course_list),
    path('api/courses/<int:pk>/', courses_views.course_detail),
    # path('api/courses/address/<str:address>/', courses_views.course_list_address)
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)