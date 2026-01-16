from mapa.models import CountryCode, Mort
from django.db.models import Sum

    #Recibe el anio de ocurrencia, la causa, y el genero (0 para indistinto)
def tripleFiltro(anio, causa, sexo):
    if (sexo != 0):
        query = Mort.objects.filter(year = anio).filter(cause=causa).filter(sex=sexo)
    else:
        query = Mort.objects.filter(year = anio).filter(cause=causa)
    return query

def generarDic(query, grupoEdad):
    dic = {}
    for c in CountryCode.objects.filter(name__startswith='A'):
        dic[c.name] = query.filter(country=c.country).aggregate(total=Sum(grupoEdad))['total']
    #else: #Quedo para sumar 2 edades, no puede contemplar parametros variables
    #    for c in CountryCode.objects.filter(name__startswith='A'):
    #    dic[c.name] = query.filter(country=c.country).aggregate(total=Sum(grupoInicio)+Sum('deaths9'))['total']
    #    query.aggregate(total=Sum('deaths10')+Sum('deaths9'))['total']
    return dic