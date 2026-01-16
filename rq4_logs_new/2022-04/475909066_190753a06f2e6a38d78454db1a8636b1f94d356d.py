from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from breed_app.models import Breed
from breed_app.api.serializers import BreedSerializer

# Create your views here.
class BreedsList(APIView):

    def get(self, request):
        breeds = Breed.objects.all()
        ## serializer just process single objet
        serializer = BreedSerializer(breeds, many=True)
        #print(dir(serializer))
        return Response(serializer.data)

    def post(self, request):
        serializer = BreedSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        else:
            return Response(serializer.errors)