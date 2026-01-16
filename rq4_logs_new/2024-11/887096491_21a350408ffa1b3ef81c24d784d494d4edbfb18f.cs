using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.DTOs.Employees;
using WolfDen.Application.Requests.Queries.Employees.GetEmployeeIdSignUp;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Employees.GetEmployeeIDSignUp
{
    public class GetEmployeeIDSignUPQueryHandler(WolfDenContext context) : IRequestHandler<GetEmployeeIDSignUpQuery, EmployeeSignUpDto>
    {
        private readonly WolfDenContext _context = context;

        public async Task<EmployeeSignUpDto> Handle(GetEmployeeIDSignUpQuery request, CancellationToken cancellationToken)
        {
            EmployeeSignUpDto result = new();
            var employee = await _context.Employees.FirstOrDefaultAsync(x => x.EmployeeCode == request.EmployeeCode && x.RFId == request.RFId, cancellationToken);
            if (employee == null)
            {

                result.Id = 0;
                result.Message = string.Format("Employee Does not Exists!");
                return result;
            }
            if (employee.Email != null)
            {

                result.Id = 0;
                result.Message = string.Format("Employee Already Exists!");
                return result;
            }

            result.Id = employee.Id;
            result.Message = string.Format($"HI {employee.FirstName} {employee.LastName} Welcome To GeekyWolf");
            return result;

        }
    }
}