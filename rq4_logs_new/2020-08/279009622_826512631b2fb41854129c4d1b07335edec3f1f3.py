from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from core.models import ProductLine, Review

@receiver(post_save, sender=Review)
@receiver(post_delete, sender=Review)
def update_product_line_rating(sender, instance, **kwargs):
    reviews = instance.product_line.review_set.all()
    num_ratings = reviews.count()
    instance.product_line.num_ratings = num_ratings
    total_ratings = 0
    rating = None
    for review in reviews:
        total_ratings += review.rating
    if num_ratings > 0:
        rating = total_ratings / num_ratings
    instance.product_line.rating = rating
    instance.product_line.save()