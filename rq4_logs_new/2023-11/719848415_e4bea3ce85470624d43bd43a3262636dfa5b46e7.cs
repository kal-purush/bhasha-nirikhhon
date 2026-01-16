using Application.Common.Exceptions;
using Application.Common.Interfaces;
using Application.Common.Security;
using MediatR;
using System.Reflection;

namespace Application.Common.Behaviours
{
    public class AuthorizeBehaviour<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse> where TRequest : notnull
    {
        private readonly IIdentityService _identityService;
        private readonly ICurrentUserService _currentUserService;

        public AuthorizeBehaviour(IIdentityService identityService, ICurrentUserService currentUserService)
        {
            _identityService = identityService;
            _currentUserService = currentUserService;
        }

        public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
        {
            var authorizeAttribute = request.GetType().GetCustomAttributes<AuthorizeAttribute>();
            if (_currentUserService.UserId is null)
            {
                throw new UnauthorizedAccessException();
            }
            var authorizeWithRoles = authorizeAttribute.Where(x => !string.IsNullOrEmpty(x.Roles));
            if (authorizeWithRoles.Any())
            {
                var authorize = false;
                foreach (var roles in authorizeWithRoles.Select(x => x.Roles.Trim(' ').Split(',')))
                {
                    foreach (var role in roles)
                    {
                        var isInRole = await _identityService.IsInRoleAsync(_currentUserService.UserId, role);
                        if (isInRole)
                        {
                            authorize = true;
                            break;
                        }
                    }
                }
                if (!authorize)
                {
                    throw new ForbiddenAccessException();
                }
            }
            var authorizeWithPolicy = authorizeAttribute.Where(x => !string.IsNullOrWhiteSpace(x.Policy));
            if (authorizeWithPolicy.Any())
            {
                foreach (var policy in authorizeWithPolicy.Select(x => x.Policy.Trim(' ')))
                {
                    var authorize = await _identityService.AuthorizeAsync(_currentUserService.UserId, policy);
                    if (!authorize)
                    {
                        throw new ForbiddenAccessException();
                    }
                }
            }
            return await next();
        }
    }
}