
from rest_framework import generics, viewsets
from rest_framework.permissions import IsAuthenticated

from comment.models import Comment
from comment.permissions import IsCommenterAuthorized
from comment.serializer import CommentSerializer


class CommentListView(generics.ListAPIView):
    queryset = Comment.objects.all()
    serializer_class = CommentSerializer
    permission_classes = [IsCommenterAuthorized,]


class CommentViewSet(viewsets.ModelViewSet):
    queryset = Comment.objects.all()
    serializer_class = CommentSerializer
    permission_classes = [IsAuthenticated, ]