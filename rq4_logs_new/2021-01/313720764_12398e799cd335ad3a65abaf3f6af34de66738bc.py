from django.test import TestCase
from django.contrib.auth.models import User
from profiles.models import UserProfile

class TestProfilesModels(TestCase):

    def setUp(self):
        new_user = User.objects.create_user(
            username='test_user', password='verysecretpassword1!'
        )

    def test_user_profile_str_method(self):
        new_user = UserProfile.objects.get(user__username='test_user')

        self.assertEqual(str(new_user), 'test_user')
    
    def tearDown(self):
        new_user = UserProfile.objects.get(user__username='test_user')

        del new_user