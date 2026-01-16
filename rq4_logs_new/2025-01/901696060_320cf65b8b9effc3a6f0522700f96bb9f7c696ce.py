from django.db.models.signals import pre_save
from django.dispatch import receiver
import os
from .models import CustomUser

@receiver(pre_save, sender=CustomUser)
def delete_old_profile_picture(sender, instance, **kwargs):
    if instance.pk:
        old_profile_picture = sender.objects.get(pk=instance.pk).profile_picture
        if old_profile_picture:
            if old_profile_picture != instance.profile_picture:
                if os.path.isfile(old_profile_picture.path):
                    os.remove(old_profile_picture.path)