using EMS.Domain.Repositories.EM;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using System.Security.Claims;

public class DepartmentManagerHandler : AuthorizationHandler<DepartmentManagerRequirement>
{
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IEmployeeRepository _employeeRepository; // Repository for Employee data

    public DepartmentManagerHandler(IHttpContextAccessor httpContextAccessor, IEmployeeRepository employeeRepository)
    {
        _httpContextAccessor = httpContextAccessor;
        _employeeRepository = employeeRepository;
    }

    protected override async Task HandleRequirementAsync(AuthorizationHandlerContext context, DepartmentManagerRequirement requirement)
    {
        var userId = context.User?.FindFirst(ClaimTypes.NameIdentifier)?.Value; // User ID from claims

        if (string.IsNullOrEmpty(userId))
        {
            context.Fail();
            return;
        }

        // Fetch the employee details from the database
        var employee = await _employeeRepository.GetEmployeeByUserIdAsync(userId);

        if (employee == null || string.IsNullOrEmpty(employee.DepartmentId))
        {
            context.Fail();
            return;
        }

        // Check if the employee's department matches the required department
        var routeDepartmentId = _httpContextAccessor.HttpContext?.Request.RouteValues["departmentId"]?.ToString();

        if (!string.IsNullOrEmpty(routeDepartmentId) && employee.DepartmentId == routeDepartmentId)
        {
            context.Succeed(requirement);
        }
        else
        {
            context.Fail();
        }
    }
}
    