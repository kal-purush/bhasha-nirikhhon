from rest_framework import viewsets

from .models import Post, Attachment
from .serializer import PostSerializer, AttachmentSerializer


class PostViewSet(viewsets.ModelViewSet):
    queryset = Post.objects.all()
    serializer_class = PostSerializer