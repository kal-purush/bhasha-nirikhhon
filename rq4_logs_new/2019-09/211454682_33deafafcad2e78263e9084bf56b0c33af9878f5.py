from django.utils.deprecation import MiddlewareMixin

from restaff.api.employee.models import Employee
from restaff.api.expert.models import Expert
from restaff.api.hr.models import Hr


class RoleObjectMiddleware(MiddlewareMixin):
    def process_request(self, request):
        assert hasattr(request, 'user'), "The Django authentication middleware requires session middleware "
        if request.path.startswith('/api/employees'):
            employee = Employee.objects.first()
            setattr(request, 'employee', employee)
        if request.path.startswith('/api/employees'):
            expert = Expert.objects.first()
            setattr(request, 'expert', expert)
        if request.path.startswith('/api/employees'):
            hr = Hr.objects.first()
            setattr(request, 'hr', hr)