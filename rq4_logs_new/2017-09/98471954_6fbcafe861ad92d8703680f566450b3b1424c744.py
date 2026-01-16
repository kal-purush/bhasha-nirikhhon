from django.conf.urls import url, include
from rest_framework import routers
from .views import ApuestasViewSet, registrar_apuesta

# Routers provide an easy way of automatically determining the URL conf.
router = routers.DefaultRouter()
router.register(r'Apuestas', ApuestasViewSet)


urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^registrar_apuesta/', registrar_apuesta),
    url(
        r'^api-auth/', include(
            'rest_framework.urls', namespace='rest_framework'
        )
    ),
]