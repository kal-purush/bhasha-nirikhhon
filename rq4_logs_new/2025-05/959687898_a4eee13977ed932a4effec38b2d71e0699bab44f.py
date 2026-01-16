from .models import Category

def categories(request):
    return {'categories': Category.objects.all()}

def cart_count(request):
    cart = request.session.get('cart', {})
    return {'cart_count': sum(cart.values())}