from app.models import Product
import json
import requests
from app.services import receipt_url, ids_oc


def get_products_for_sale():
    productos = {}
    for producto in Product.objects.raw('SELECT sku, name FROM app_product'):
        if len(producto.sku) > 4:
            productos[producto.sku] = producto.name
    return productos


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        print("returning FORWARDED_FOR")
        ip = x_forwarded_for.split(',')[-1].strip()
    elif request.META.get('HTTP_X_REAL_IP'):
        print("returning REAL_IP")
        ip = request.META.get('HTTP_X_REAL_IP')
    else:
        print("returning REMOTE_ADDR")
        ip = request.META.get('REMOTE_ADDR')
    return ip


def update_dict(dict, new_key, new_value):
    if new_key in dict:
        dict[new_key] += new_value
    else:
        dict[new_key] = new_value

    return dict


def add_to_cart_file(request, sku, quantity, ip):
    file_name = str(ip)+".json"
    agregado = {sku:int(quantity)}
    try:
        with open(file_name) as outfile:
            data = json.load(outfile)

        update_dict(data, sku, int(quantity))
        with open(file_name, 'w+') as outfile:
            json.dump(data, outfile)

    except FileNotFoundError:
        with open(file_name, 'w+') as outfile:
            json.dump(agregado, outfile)


def update_cart_file(request, sku, quantity, ip):
    file_name = str(ip)+".json"
    nuevo = {sku:int(quantity)}
    with open(file_name) as outfile:
        data = json.load(outfile)
    if int(quantity) > 0:
        data.update(nuevo)
    else:
        data.pop(sku, None)
    with open(file_name, 'w+') as outfile:
        json.dump(data, outfile)


def sku_with_name(cart):
    productos = get_products_for_sale()
    resultado = {}
    for sku in cart:
        resultado.update({sku: (productos[sku],cart[sku])})
    return resultado


def address_to_coordinates(calle, numero):
    bla = requests.get("https://api.mapbox.com/geocoding/v5/mapbox.places/{}%20{}.json?types=address&proximity=-70.6693,-33.4489&access_token=pk.eyJ1Ijoic25vcmxheDgiLCJhIjoiY2p4YjZ6aXF1MDN3cTNwbG94cnExcXFjYSJ9.DdB8WOhH8k6kyL_jFrC2-Q".format(calle, numero))
    return json.loads(bla.text)['features'][0]['center']


def receipt_creation(cliente, precio):
    url = receipt_url+'/sii/boleta'
    headers = {'Content-Type': 'application/json'}
    body = {'proveedor': ids_oc[13], 'cliente': cliente, 'total': precio}
    result = requests.post(url, data=json.dumps(body), headers=headers)
    boleta = json.loads(result.text)
    return boleta


if __name__ == '__main__':
    pass