from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin

from rest_framework import viewsets

from .models import Post
from .serializers import PostModelSerializer


class PostModelViewSet(viewsets.ModelViewSet):
    serializer_class = PostModelSerializer
    queryset = Post.objects.all()