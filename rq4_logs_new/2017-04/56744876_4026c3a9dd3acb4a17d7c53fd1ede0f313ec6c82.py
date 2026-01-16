from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseNotFound, HttpResponseRedirect
from .models import Url


def mostrar_Urlcorta(request, URLcorta):
    try:
        elemento = Url.objects.get(id=int(URLcorta))
        respuesta = '<meta http-equiv="Refresh" content="2;url='
        respuesta += elemento.urlLarga + '">'
        #return HttpResponseRedirect(elemento.urlLarga)
    except Url.DoesNotExist:
        respuesta = "Recurso no disponible"
    return HttpResponse(respuesta)


@csrf_exempt
def process(request):
    formulario = "<form method = POST action=''" + ">"
    formulario += "URL: <input type='text' name='url'><br/>"
    formulario += "<input type='submit' value='Enviar'></form>"
    cuerpo = request.body
    lURLs = Url.objects.all()
    if (request.method == "GET"):
        respuesta = formulario + "<ul>"
        for url in lURLs:
            respuesta += "<li>" + url.urlLarga + " --> " + request.get_host()
            respuesta += request.path + str(url.id) + "</li>"
        respuesta += "</ul>"
    elif (request.method == "POST"):
        cuerpo = request.POST['url']
        if cuerpo[0:4] != "http":  # Incluye http:// si es necesario
            cuerpo = "http://" + cuerpo
        try:
            elemento = Url.objects.get(urlLarga=cuerpo)
            respuesta = "Elemento ya almacenado</br>"
        except Url.DoesNotExist:
            elemento = Url(urlLarga=cuerpo)
            elemento.save()
            respuesta = "Elemento nuevo creado</br>"
        respuesta += '<a href="' + elemento.urlLarga + '">'
        respuesta += str(elemento.id) + '</a>'": "
        respuesta += '<a href="' + elemento.urlLarga + '">'
        respuesta += elemento.urlLarga + '</a>'
    else:
        respuesta = "Metodo introducido no válido"
    return HttpResponse(respuesta)

def el404(request):
    return HttpResponseNotFound('Error 404: Recurso no válido.')