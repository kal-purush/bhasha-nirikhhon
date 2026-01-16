from django.db import models
from django.utils import timezone

from oscar.core.compat import AUTH_USER_MODEL

class Timestamp(models.Model):

    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class AbstractCategory(Timestamp):

    name = models.CharField(max_length=64)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name

class AbstractPost(Timestamp):
    post_title = models.CharField(max_length=200)
    post_content = models.TextField(blank=True)
    featured_image = models.ImageField(_('Featured Image'),upload_to='images',blank=True,null=True,max_length=255)
    post_date = models.DateTimeField('Post Date')
    categories = models.ManyToManyField(
        AbstractCategory,
        through='AbstractCategoryMapping',
        through_fields=('post', 'category'),
    )
    category = models.ManyToManyField(
        AbstractCategory, blank=True, through='AbstractCategoryMapping', verbose_name=_("Category"))
    excerpt = models.TextField(blank=True)
    post_excerpt = models.CharField(max_length=200)

    class Meta:
        ordering = ['-post_date', 'title']

    def __str__(self):
        return self.post_title

class AbstractCategoryMapping(Timestamp):
    post = models.ForeignKey(AbstractPost, on_delete=models.CASCADE)
    category = models.ForeignKey(AbstractCategory, on_delete=models.CASCADE)
    
    def __str__(self):
        return "%s-%s" % self.post,self.category