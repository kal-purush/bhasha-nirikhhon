from django.db.models.signals import post_save
from django.dispatch import receiver
from finance.models import Payroll
from django.template.loader import render_to_string
from user.tasks import send_email


@receiver(post_save, sender=Payroll)
def send_payroll_email(sender, instance, created, **kwargs):
    if not created:
        user_email = instance.employee.email
        context = {
            'employee': instance.employee,
            'month': instance.month,
            'year': instance.year,
            'released': instance.released,
            'basic_salary': instance.basic_salary
        }
        email_subject = 'Payroll Released'
        template_file_path = "emails/salary_slip.html"

        email_body = render_to_string(template_file_path, context)
        #
        data = {
            'email_body': email_body,
            'to_email': user_email,
            'email_subject': email_subject
        }
        send_email(data)
    if created:
        employee = instance.employee
        if employee:
            email_body = 'Hi ' + employee.username + '!\n' + \
                         ' Your payroll has been generated for the month of ' + instance.month \
                         + ' ' + instance.year + '\n' + \
                         'You can now view it at your dashboard.'
            data = {'email_body': email_body, 'to_email': employee.email,
                    'email_subject': 'Payroll Generated'}
            # generate_and_send_employee_credentials(data)
            send_email.delay(data)
            instance.employee = employee
            # return super().create(instance)
        else:
            raise ('Due to some technical difficulty, an error is raised')
