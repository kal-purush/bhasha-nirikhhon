from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class DjangoSocial(AppConfig):
    """Main app"""
    name = 'django_social'
    verbose_name = _('Django Social')


class Chats(AppConfig):
    name = 'django_social.contrib.chats'
    verbose_name = _('Chat')


class Users(AppConfig):
    name = 'django_social.contrib.users'
    verbose_name = _('Users')