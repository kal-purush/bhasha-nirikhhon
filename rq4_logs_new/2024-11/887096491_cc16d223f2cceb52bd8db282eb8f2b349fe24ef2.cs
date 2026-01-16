using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.DTOs.Employees;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Employees.EmployeeLogin
{
    public class EmployeeLoginQueryHandler(WolfDenContext context) : IRequestHandler<EmployeeLoginQuery, EmployeeSignUpDto> 
    {
        private readonly WolfDenContext _context=context;


        public async Task<EmployeeSignUpDto>Handle(EmployeeLoginQuery request, CancellationToken cancellationToken)
        {
            EmployeeSignUpDto result = new();
            var employee = await _context.Employees.FirstOrDefaultAsync(x => x.Email == request.Email && x.Password == request.Password, cancellationToken);
                if (employee == null) {
                    result.Id = 0;
                    result.status = false;
                    return result;
                }
                else
                {
                    result.Id = employee.Id;
                    result.status = true;
                    return result;
                }
            }
        }
    }
