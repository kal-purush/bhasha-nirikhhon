# # rootapp/routing.py
# from channels.auth import AuthMiddlewareStack
# from channels.routing import ProtocolTypeRouter, URLRouter
# import signlanguage.routing

# application = ProtocolTypeRouter({
#     # (http->django views is added by default)
#     'websocket': AuthMiddlewareStack(
#         URLRouter(
#             signlanguage.routing.websocket_urlpatterns
#         )
#     ),
# })

# routing.py

# routing.py

# from channels.routing import ProtocolTypeRouter, URLRouter
# from django.urls import path

# from . import consumers

# application = ProtocolTypeRouter({
#     'websocket': URLRouter([
#         path('ws/signlanguage/', consumers.SignLanguageConsumer.as_asgi()),
#     ]),
# })

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
import signlanguage.routing

application = ProtocolTypeRouter({
    # (http->django views is added by default)
    'websocket': AuthMiddlewareStack(
        URLRouter(
            signlanguage.routing.websocket_urlpatterns
        )
    ),
})