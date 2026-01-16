from ..models import *
from rest_framework import serializers

class UsuarioSerializer():
    class Meta:
        model = Usuario
        fields = '__all__'


class ComentarioSerializer():
    class Meta:
        model = Comentario
        fields = '__all__'


class PontoTuristicoSerializer():
    class Meta:
        model = PontoTuristico
        fields = '__all__'


class EstabelecimentoSerializer():
    class Meta:
        model = Estabelecimento
        fields = '__all__'


class GuiaTuristicoSerializer():
    class Meta:
        model = GuiaTuristico
        fields = '__all__'


class EventoSerializer():
    class Meta:
        model = Evento
        fields = '__all__'