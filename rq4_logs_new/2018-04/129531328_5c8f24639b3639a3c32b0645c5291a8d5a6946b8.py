from django.contrib.auth.models import User
from django.db import models


class AuditModel(models.Model):
    """
    Base model for others, providing creation, insertion, and update times
    """
    created_on = models.DateTimeField(auto_now_add=True)
    modified_on = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UserProfile(AuditModel):
    """
    Store additional information on users
    """
    user = models.OneToOneField(User)
    phone_num = models.CharField(max_length=20, null=True, default=None)


class Event(AuditModel):
    """
    Event to volunteer for
    """
    name = models.CharField(max_length=255)
    users = models.ManyToManyField(User, related_name='user_events')


class Location(AuditModel):
    """
    Location of users and events
    """
    zipcode = models.CharField(max_length=16)
    users = models.ManyToManyField(User, related_name='user_locations')
    events = models.ManyToManyField(Event, related_name='event_locations')


class RoleType(AuditModel):
    """
    Names of Roles users and events can have
    """
    name = models.CharField(max_length=255, unique=True)


class UserEventRole(AuditModel):
    """
    Relationships between Users and Events
    """
    role = models.ForeignKey(RoleType)
    user = models.ForeignKey(User)
    event = models.ForeignKey(Event)
    active = models.BooleanField(default=True)


class Description(AuditModel):
    """
    Addition information for Users and Events
    """
    event = models.ForeignKey(Event, null=True)
    user = models.ForeignKey(User, null=True)
    value = models.CharField(max_length=255, blank=False)
    url = models.CharField(max_length=255)
    image = models.ImageField(blank=True)