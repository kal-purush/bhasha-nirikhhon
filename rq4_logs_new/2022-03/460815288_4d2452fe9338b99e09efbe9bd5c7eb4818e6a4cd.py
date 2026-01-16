from currency import views as currency_views

from django.urls import path

app_name = 'currency'


urlpatterns = [
    path('contacts/list/', currency_views.ContactUsList.as_view(), name='contactus_list'),
    path('rate/list/', currency_views.RateList.as_view(), name='rate_list'),
    path('source/list/', currency_views.SourceList.as_view(), name='source_list'),
    path('source/create/', currency_views.SourceCreate.as_view(), name='source_create'),
    path('source/update/<int:pk>/', currency_views.SourceUpdate.as_view(), name='source_update'),
    path('source/detail/<int:pk>/', currency_views.SourceDetail.as_view(), name='source_detail'),
    path('source/delete/<int:pk>/', currency_views.SourceDelete.as_view(), name='source_delete'),
]