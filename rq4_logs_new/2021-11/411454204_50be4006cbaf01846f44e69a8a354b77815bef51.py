from django.core import serializers
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from orders.models import ProductOrder


@csrf_exempt
def update_product_order_view_API(request):
    id = request.POST.get('id', '')
    field = request.POST.get('field', '')
    value = request.POST.get('value', '')
    productOrder = ProductOrder.objects.get(id_product_order=id)

    if field == "reference":
        productOrder.reference = value
    if field == "color":
        productOrder.color = value
    if field == "size":
        productOrder.size = value
    if field == "quantity":
        productOrder.quantity = value
    if field == "line":
        productOrder.line = value
    if field == "brand":
        productOrder.brand = value
    if field == "collection":
        productOrder.collection = value
    if field == "price":
        productOrder.price = value
    if field == "total_price":
        productOrder.total_price = value
    if field == "cost":
        productOrder.cost = value
    if field == "total_cost":
        productOrder.total_cost = value
    if field == "status":
        productOrder.status = value

    productOrder.save()
    return JsonResponse({"success": "Updated"})