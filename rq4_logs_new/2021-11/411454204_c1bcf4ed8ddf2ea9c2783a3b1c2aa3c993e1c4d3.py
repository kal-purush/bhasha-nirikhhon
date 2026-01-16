from django.db.models import query
from django.http.response import HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from django.contrib import messages
import pandas as pd


from datetime import date

from orders.models import *
from orders.ServicesOrders import *


class DynamicQueryView(View):
    template_name = 'orders/orders.html'
    template_name = 'orders/query_dynamic.html'

    def get(self, request, **kwargs):
        orders = Order.objects.filter(date__gte=date(2021, 7, 29))
        products_order = ProductOrder.objects.filter(id_order_id__in=orders)
        values = ServicesReadPivot(
            orders.values(), products_order.values()).data()
        data = {
            'values': values
        }
        return render(request, self.template_name, data)