from django.contrib import admin
from .models import *

# Register your models here.


class CarritoAdmin(admin.ModelAdmin):
    readonly_fields = ('totalCarrito',)


class DescuentoAdmin(admin.ModelAdmin):
    readonly_fields = ('fechaCreacion', 'fechaModificacion')


class ProductoPublicoAdmin(admin.ModelAdmin):
    readonly_fields = ('fechaCreacion', 'fechaModificacion')


class ItemsPedidoAdmin(admin.ModelAdmin):
    readonly_fields = ('fechaCreacion', 'fechaModificacion')


class DetallePedidoAdmin(admin.ModelAdmin):
    readonly_fields = ('fechaCreacion', 'fechaModificacion', 'total')


admin.site.register(Carrito, CarritoAdmin)
admin.site.register(Descuento, DescuentoAdmin)
admin.site.register(CategoriaProducto)
admin.site.register(ProductoPublico, ProductoPublicoAdmin)
admin.site.register(ItemCarrito)
admin.site.register(ItemsPedido, ItemsPedidoAdmin)
admin.site.register(DetallePedido, DetallePedidoAdmin)