from django import template
from cromio.models import Product

register = template.Library()

@register.inclusion_tag('menu.html')
def menu():
    menu_products = Product.objects.filter(menu_position__isnull=False).order_by('menu_position')
    return {'menu_products': menu_products}