from presta.api.prestapi import Prestapi
from django.conf import settings
from .models import PrestaConfig


def get_dataApi():
    sesion = PrestaConfig.objects.filter(pk=settings.SESION_PRESTA)[0]
    objApi = Prestapi(host=sesion.host,
                      protocol=sesion.protocol,
                      key=sesion.key)
    result = objApi.get_rules(resource='addresses')
    print(result)
    return result[1]


def create_model():
    sesion = PrestaConfig.objects.filter(pk=settings.SESION_PRESTA)[0]
    objApi = Prestapi(host=sesion.host,
                      protocol=sesion.protocol,
                      key=sesion.key)
    result = objApi.get_rules(resource='addresses')[1]
    return result