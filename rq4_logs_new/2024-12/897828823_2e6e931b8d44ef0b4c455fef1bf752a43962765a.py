from django import template
# from ..models import Products

# from django.template.exceptions.Template

register=template.Library()

@register.filter(name="chunks")
def chunks(list_data,chunk_size):
    chunk=[]
    i=0
    for data in list_data:
        chunk.append(data)
        i+=1
        if i==chunk_size:
            i=0
            yield chunk
            chunk=[]
            
    if chunk:
        yield chunk
    