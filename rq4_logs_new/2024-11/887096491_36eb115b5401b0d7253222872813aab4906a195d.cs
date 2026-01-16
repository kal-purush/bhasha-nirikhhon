using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WolfDen.Application.DTOs.Employees;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Services
{
    public class GetEmployeeService(WolfDenContext context)
    {
        private readonly WolfDenContext _context = context;
        public async Task<EmployeeHierarchyDto> GetEmployee(Employee employee, CancellationToken cancellationToken)
        {
            EmployeeHierarchyService service = new(_context);
            EmployeeHierarchyDto employeeDto = new()
                {
                    Id = employee.Id,
                    EmployeeCode = employee.EmployeeCode,
                    FirstName = employee.FirstName,
                    LastName = employee.LastName,
                    Email = employee.Email,
                    PhoneNumber = employee.PhoneNumber,
                    DateofBirth = employee.DateofBirth,
                    DepartmentId = employee.DepartmentId,
                    DepartmentName = employee.Department != null ? employee.Department.Name : null,
                    DesignationId = employee.DesignationId,
                    DesignationName = employee.Designation != null ? employee.Designation.Name : null,
                    ManagerId = employee.ManagerId,
                    ManagerName = employee.Manager != null
                       ? $"{employee.Manager.FirstName}{(string.IsNullOrWhiteSpace(employee.Manager.LastName) ? "" : " " + employee.Manager.LastName)}"
                       : null,
                    IsActive = employee.IsActive,
                    Address = employee.Address,
                    Country = employee.Country,
                    State = employee.State,
                    EmploymentType = employee.EmploymentType,
                    Photo = employee.Photo,
                    Subordinates = await service.GetSubordinates(employee.Id, cancellationToken)
                };
            return employeeDto;
        }
    }
}