from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework import mixins
from rest_framework import generics

from .serializers import PersonSerializer
from .models import Person


class PersonViewV1(APIView):
    def get(self, request, person_id: int):
        person = get_object_or_404(Person, id=person_id)
        serialized = PersonSerializer(person)
        return Response(serialized.data)

    def post(self, request, person_id: int, age: int):
        person = get_object_or_404(Person, id=person_id, age=age)
        serialized = PersonSerializer(person, data=request.data)

        if serialized.is_valid():
            serialized.save()
            return Response(serialized.data)
        return Response(serialized.errors, status=status.HTTP_400_BAD_REQUEST)


# ----


# ----


class PersonViewV2(
    mixins.RetrieveModelMixin,
    mixins.UpdateModelMixin,
    generics.GenericAPIView,
):
    # queryset = Person.objects.all()
    serializer_class = PersonSerializer

    def get_queryset(self):
        age = self.kwargs.get("age")
        return Person.objects.filter(age=age)

    def get(self, request, *args, **kwargs):
        return self.retrieve(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.update(request, *args, **kwargs)