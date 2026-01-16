from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from core.models import Review

@receiver(post_save, sender=Review)
@receiver(post_delete, sender=Review)
def update_product_rating(sender, instance, **kwargs):
    reviews = instance.product.review_set.all()
    num_reviews = reviews.count()
    instance.product.num_reviews = num_reviews
    total_rating = 0
    rating = None
    for review in reviews:
        total_rating += review.rating
    if num_reviews > 0:
        rating = total_rating / num_reviews
    instance.product.rating = rating
    instance.product.save()