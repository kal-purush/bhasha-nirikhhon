using Core.Abstractions.Services;
using Domain;
using Microsoft.AspNetCore.Authorization;
using Services.Exceptions;
using WebShop.Authorization.Requirements;

namespace WebShop.Authorization.Handlers
{
    public class RequireAdminHandler : AuthorizationHandler<AdminRoleRequirement>
    {
        private readonly IUsersService _usersService;
        private readonly HttpContext _httpContext;

        public RequireAdminHandler(
            IUsersService usersService,
            IHttpContextAccessor httpContextAccessor)
        {
            _usersService = usersService;
            _httpContext = httpContextAccessor.HttpContext;

            ArgumentNullException.ThrowIfNull(_httpContext);
        }

        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context, AdminRoleRequirement requirement)
        {
            var userId = _httpContext.Session.GetInt32("UserId");

            if (userId == null)
                throw new NotAuthorizedException();

            var loggedInUser = _usersService.GetById(userId.Value);

            foreach (var role in loggedInUser.Roles)
            {
                if (role == UserRole.Administrator)
                {
                    context.Succeed(requirement);
                    return Task.CompletedTask;
                }
            }

            throw new NotAuthorizedException();
        }
    }
}