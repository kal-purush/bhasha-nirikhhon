using MediatR;
using System.Threading;
using System.Threading.Tasks;
using WebApi.Common.Exceptions;
using WebApi.Persistence;
using WebApi.Services.Interfaces;

namespace WebApi.Common.Behaviours.Authorization
{
    public class AuthorizationCheckBehaviour<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse> where TRequest : IRequest<TResponse>
    {
        private readonly ForehandContext _db;
        private readonly ICurrentUserService _currentUserService;
        private readonly IAuthorizationCheck<TRequest> _authorizationCheck;

        public AuthorizationCheckBehaviour(ForehandContext db, ICurrentUserService currentUserService, IAuthorizationCheck<TRequest> authorizationCheck = null)
        {
            _db = db;
            _authorizationCheck = authorizationCheck;
            _currentUserService = currentUserService;
        }

        public async Task<TResponse> Handle(TRequest request, CancellationToken cancellationToken, RequestHandlerDelegate<TResponse> next)
        {
            if (_authorizationCheck is null || await _authorizationCheck.IsAuthorized(request, _db, _currentUserService, cancellationToken))
                return await next();

            // if authorization fails because of expired bearer, 401 should be returned
            if (string.IsNullOrEmpty(_currentUserService.UserId))
                throw new Unauthorized401Exception();

            throw new Forbidden403Exception();
        }
    }
}