import stripe
from .models import Order, Tax

# test key
stripe.api_key = 'sk_test_4eC39HqLyjWDarjtT1zdp7dc'

DOMAIN = 'http://127.0.0.1:8000'


def create_stripe_checkout_session(order):
    tax_rates = []
    if order.tax:
        tax_rates.append(create_stripe_tax(order.tax.rate))

    discounts = []
    if order.discount:
        discounts.append({
            'coupon': create_stripe_discount(order.discount.rate)
        })

    items = []
    for item in order.items.all():
        items.append({
                'price_data': {
                    'currency': 'rub',
                    'unit_amount': item.price * 100,
                    'product_data': {
                        'name': item.name
                    }

                },
                'quantity': 1,
                'tax_rates': tax_rates,
            })

    checkout_session = stripe.checkout.Session.create(
        line_items=items,
        mode='payment',
        success_url=DOMAIN,
        cancel_url=DOMAIN + '/cart',
        discounts=discounts,
    )

    return checkout_session.id


def create_stripe_tax(rate):
    tax = stripe.TaxRate.create(
        display_name='NDS',
        description='NDS Russia',
        jurisdiction='RU',
        percentage=rate,
        inclusive=True,
    )

    return tax.id


def create_stripe_discount(rate):
    coupon = stripe.Coupon.create(
        percent_off=rate,
        duration='forever',
    )

    return coupon.id


def get_or_create_order(request):
    if not request.session.session_key:
        request.session.create()
    order, created = Order.objects.get_or_create(session_key=request.session.session_key)
    order.tax = Tax.objects.get(rate=10)
    order.save()

    return order