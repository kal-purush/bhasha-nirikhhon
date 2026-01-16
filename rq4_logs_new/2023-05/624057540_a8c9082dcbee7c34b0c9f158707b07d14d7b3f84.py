from django import template
from pages.models import Category

register = template.Library()


def put_mask(value):
    """Puts mask +7(XXX) XXX-XX-XX to the text - special for username"""
    text = str(value)
    if len(text) == 10:
        text = "+7 (" + text[0:3] + ") " + text[3:6] + "-" + text[6:8] + "-" + text[8:]
    return text


register.filter('put_mask', put_mask)


@register.inclusion_tag("nav_pages_list.html")
def nav_pages_list(is_auth):
    qs = Category.objects.prefetch_related('pages').all().order_by('position')
    if not is_auth:
        qs = qs.filter(public=True)
    return {"navlist": qs}