from rest_framework import status
from django.core import mail
from django.urls import reverse
from finance.models import Payroll

def test_payroll_update(admin_factory, employee_factory, payroll_factory,  authed_token_client_generator, mailoutbox, celery_eager_run_on_commit):
    employee = employee_factory(email='employee@gmail.com')
    admin_user = admin_factory()
    payroll = payroll_factory(employee=employee)
    client = authed_token_client_generator(admin_user)
    data = {
        'employee': str(employee.id),
        'basic_salary': 2000,
        'month': 'JUNE',
        'year': '2023',
        'released': True
    }

    response = client.put(reverse('payroll-detail', kwargs = {'pk': payroll.id}), data=data, format='json')
    assert response.status_code == status.HTTP_200_OK
    assert Payroll.objects.filter(id=response.data['id']).exists()
    assert len(mail.outbox) == 1  # Assuming only one email is sent
    email = mail.outbox[0]
    assert email.subject == 'Payroll Released'
    assert email.to == [employee.email]