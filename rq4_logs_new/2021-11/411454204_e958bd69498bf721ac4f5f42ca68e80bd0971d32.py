from django.http.response import HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from django.views.generic import UpdateView
from django.contrib import messages
from django.urls import reverse_lazy

from datetime import datetime

from orders.models import *
from orders.ServicesOrders import *
from orders.forms import ProductOrderFrom


class ProductsOrderView(View):
    template_name = 'orders/orders_products.html'

    def get(self, request):
        query = ProductOrder.objects.all()
        data = {
            'products_order': query
        }

        return render(request, self.template_name, data)


class UpdateProductOrderView(UpdateView):
    template_name = 'orders/update_product_order.html'
    form_class = ProductOrderFrom
    queryset = ProductOrder.objects.all()
    pk_url_kwarg = 'id_product_order'

    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        self.success_url = reverse_lazy(
            'order:detail_order', kwargs={'id_order': self.object.id_order_id})
        messages.success(request, f'Se modificó producto con ID {self.object}')
        return super().post(request, *args, **kwargs)