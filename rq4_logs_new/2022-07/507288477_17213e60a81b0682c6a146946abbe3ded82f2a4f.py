from __future__ import absolute_import, unicode_literals
from django.db.models import Q
from celery import shared_task

from src.dealership.models import Dealership, ProviderCars
from src.customer.models import Customer, Buy


@shared_task
def customer_buy_car():
    Customers = Customer.objects.all()

    for customer in Customers:
        query = customer.query
        cars = ProviderCars.objects.exclude(cars__isnull=True, dealership__isnull=True).filter(
            Q(cars__model=query.get('model')) &
            Q(cars__price__lte=query.get('price')) &
            Q(cars__engine_volume__lte=query.get('engine')) &
            Q(cars__color=query.get('color')))
        car = cars.order_by('cars__price').first()
        if car is None:
            continue
        else:
            Buy.objects.create(customer=customer, dealership=car.dealership, car=car.cars)
            customer.balance -= car.cars.price
            car.dealership.balance += car.cars.price
            customer.save()
            car.dealership.save()