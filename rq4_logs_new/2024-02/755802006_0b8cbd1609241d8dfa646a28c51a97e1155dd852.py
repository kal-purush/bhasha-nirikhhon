from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import CompanyMemberViewSet

router = DefaultRouter()

router.register(r'', CompanyMemberViewSet, basename='company-members')


urlpatterns = [
    path('', include(router.urls)),
    path('<int:company_id>', CompanyMemberViewSet.as_view({'get': 'list'}), name='company-members'),
]