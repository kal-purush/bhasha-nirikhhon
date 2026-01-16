from urllib import request
from django.contrib import auth
from django.core.mail import EmailMultiAlternatives
from django.template import Context
from django.template.loader import get_template
from main.models import Order


class SendMessage:
    def order_success(request):
        html_order = get_template('email_order.html', )
        order_html = Context({'order': Order.objects.last(),
                                      'username': request.user.username})
        html_content = html_order.render(order_html)
        subject = 'Order'
        from_email = 'natalia.l.kushnir@gmail.com'
        to = auth.get_user(request).email
        msg = EmailMultiAlternatives(subject, '', from_email, [to])
        msg.attach_alternative(html_content, "text/html")
        msg.send()
        return msg

    def register_success(request):
        html_order = get_template('email_register.html', )
        order_html = Context({'order': Order.objects.last(),
                                      'username': request.user.username})
        html_content = html_order.render(order_html)
        subject = 'Register'
        from_email = 'natalia.l.kushnir@gmail.com'
        to = auth.get_user(request).email
        msg = EmailMultiAlternatives(subject, '', from_email, [to])
        msg.attach_alternative(html_content, "text/html")
        msg.send()
        return msg