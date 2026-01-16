using EMS.Domain.Models.Account;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using System.Security.Claims;

public class RoleRequirementHandler : AuthorizationHandler<RoleRequirement>
{
    private readonly UserManager<User> _userManager;

    public RoleRequirementHandler(UserManager<User> userManager)
    {
        _userManager = userManager;
    }

    protected override async Task HandleRequirementAsync(AuthorizationHandlerContext context, RoleRequirement requirement)
    {
        var userId = context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

        if (string.IsNullOrEmpty(userId))
        {
            context.Fail();
            return;
        }

        // Fetch the user from the database
        var user = await _userManager.FindByIdAsync(userId);
        if (user == null)
        {
            context.Fail();
            return;
        }

        // Check if the user has the required role
        var roles = await _userManager.GetRolesAsync(user);
        if (roles.Contains(requirement.RequiredRole))
        {
            context.Succeed(requirement);
        }
        else
        {
            context.Fail();
        }
    }
}