from django.urls import path
from . import views
app_name='show'

urlpatterns = [
    path('home/', views.home, name='home'),
    path('post/<int:post_id>/', views.detail, name='detail'),
    path('exhibition/', views.exhibition, name="exhibition"),
    path('musical/', views.musical, name='musical'),
    path('concert/', views.concert, name='concert'),
    path('classic/', views.classic, name='classic'),
    path('post/<int:post_id>/comment/', views.add_comment, name='add_comment'),
    path('comment/<int:comment_id>/edit/', views.comment_edit, name= 'comment_edit'),
    path('comment/<int:comment_id>/delete', views.comment_delete, name='comment_delete'),
    path('comment/<int:comment_id>/like/', views.post_like, name='post_like'),
    path('comment/<int:comment_id>/dislike/', views.post_dislike, name='post_dislike'),
]