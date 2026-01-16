from django.urls import path
from orders import views

urlpatterns = [
    path('',  views.Panel.as_view(), name='panel_order'),
    path('add_order/', views.AddOrderView.as_view(), name='add_order'),
    path('products/', views.ProductsOrderView.as_view(),
         name='oders_products'),
    path('orders/', views.OrdersView.as_view(),
         name='orders'),
    path('orders/detail_order/<int:id_order>', views.FilterForOrderView.as_view(),
         name='orders'),
    path('add_files/', views.AddFilesView.as_view(),
         name='add_files'),
]