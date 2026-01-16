from django.test import TestCase
from bucketlistapp.models import Bucketlist, BucketlistItem
from django.contrib.auth.models import User


class BucketModelsTest(TestCase):

    def setUp(self):
        self.user = User.objects.create(username='awesome', password='awesometest')
        self.bucketlist = Bucketlist.objects.create(name='test_bucketlist', created_by=self.user)
        self.item = BucketlistItem.objects.create(name='test_bucketitem', bucketlist=self.bucketlist)

    def tearDown(self):
        User.objects.all().delete()
        Bucketlist.objects.all().delete()
        BucketlistItem.objects.all().delete()

    def test_user_was_created_successfully(self):
        self.assertEqual(self.user.get_username(), 'awesome')
        self.assertIsInstance(self.user, User)

    def test_bucketlist_was_created_successfully(self):
        self.assertIsInstance(self.bucketlist, Bucketlist)
        self.assertTrue(Bucketlist.objects.all())

    def test_itemm_was_created_successfully(self):
        self.assertIsInstance(self.item, BucketlistItem)
        self.assertTrue(BucketlistItem.objects.all())