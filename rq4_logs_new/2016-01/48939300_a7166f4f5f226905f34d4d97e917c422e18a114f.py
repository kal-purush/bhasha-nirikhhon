from rest_framework.routers import DefaultRouter


from . import views


veggy_pi_router = DefaultRouter()
veggy_pi_router.register(r'rpipin', views.RPiPinViewSet)
veggy_pi_router.register(r'users', views.UserViewSet)
