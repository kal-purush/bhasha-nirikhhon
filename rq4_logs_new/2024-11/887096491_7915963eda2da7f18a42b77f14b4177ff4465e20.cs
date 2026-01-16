using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.DTOs.Employees;
using WolfDen.Application.Requests.Services;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Employees.GetEmployeeTeam
{
    public class GetEmployeeTeamQueryHandler(WolfDenContext context) : IRequestHandler<GetEmployeeTeamQuery, List<EmployeeHierarchyDto>>
    {
        private readonly WolfDenContext _context = context;
        public async Task<List<EmployeeHierarchyDto>> Handle(GetEmployeeTeamQuery request, CancellationToken cancellationToken)
        {
            EmployeeHierarchyService service = new(_context);
            List<EmployeeHierarchyDto> teamList = new();
            var employee = await _context.Employees.FindAsync(request.Id, cancellationToken);
            var myTeam = await _context.Employees.Where(x => x.ManagerId == request.Id).ToListAsync();
            if (myTeam.Count == 0)
            {
                var teamMates = _context.Employees.Where(x => x.ManagerId == employee.ManagerId);

                foreach (var teamMate in teamMates)
                {
                    EmployeeHierarchyDto employeeDto = new()
                    {
                        Id = teamMate.Id,
                        EmployeeCode = teamMate.EmployeeCode,
                        FirstName = teamMate.FirstName,
                        LastName = teamMate.LastName,
                        Email = teamMate.Email,
                        PhoneNumber = teamMate.PhoneNumber,
                        DateofBirth = teamMate.DateofBirth,
                        DepartmentId = teamMate.DepartmentId,
                        DesignationId = teamMate.DesignationId,
                        ManagerId = teamMate.ManagerId,
                    };
                    teamList.Add(employeeDto);
                }
                return teamList;
            }

            EmployeeHierarchyDto result = new()
            {

                Id = employee.Id,
                EmployeeCode = employee.EmployeeCode,
                FirstName = employee.FirstName,
                LastName = employee.LastName,
                Email = employee.Email,
                DateofBirth = employee.DateofBirth,
                DepartmentId = employee.DepartmentId,
                DesignationId = employee.DesignationId,
                ManagerId = employee.ManagerId,
                PhoneNumber = employee.PhoneNumber,
                Subordinates = await service.GetSubordinates(employee.Id),
            };
            teamList.Add(result);
            return teamList;
        }


    }
}