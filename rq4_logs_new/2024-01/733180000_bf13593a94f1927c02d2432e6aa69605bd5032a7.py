from rest_framework import routers

from .views import (
    JokesViewSet,
    AccountJokesViewSet,
    FavouriteJokesViewSet,
    SeenJokesViewSet,
)

router = routers.DefaultRouter()
router.register('all', JokesViewSet, basename='jokes-all'),
router.register('account', AccountJokesViewSet, basename='jokes-account'),
router.register('favourite', FavouriteJokesViewSet, basename='jokes-favourite'),
router.register('seen', SeenJokesViewSet, basename='jokes-seen'),

urlpatterns = [
]

urlpatterns += router.urls