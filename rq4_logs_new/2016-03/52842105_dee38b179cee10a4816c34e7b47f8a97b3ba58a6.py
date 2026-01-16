# -*- coding: utf-8 -*-

from django.test import TestCase
from blog.models import Post
from blog.views import PostCreateView
from django.contrib.auth.models import User


class PostTest(TestCase):

    def setUp(self):
        self.user = User.objects.create_user(
            'rawser',
            'me@raw.com',
            'rawraw'
        )
        self.user.save()

        self.post = Post(
            title='Raw Post',
            content='Rawsrawz rawaw raw raw',
            author=self.user
        )
        self.post.save()

    def test_create_post(self):
        posts = Post.objects.all()
        self.assertEqual(len(posts), 1)

    def test_post_author(self):
        post = Post.objects.filter(author__pk=self.user.pk)
        self.assertNotEqual(post, None)


class PostCreateViewTest(TestCase):

    def test_case(self):
        pass