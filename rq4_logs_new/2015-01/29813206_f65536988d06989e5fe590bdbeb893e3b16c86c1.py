from django import template
register = template.Library()

@register.filter(name='cons')
def cons(pk):
	return pk.encode('utf8')