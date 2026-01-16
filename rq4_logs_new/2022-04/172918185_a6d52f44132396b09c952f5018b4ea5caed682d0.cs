#if NETCOREAPP
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Nexus.Link.Contracts.Misc.AspNet.Sdk;
using Nexus.Link.Contracts.Misc.Sdk.Authentication;
using Nexus.Link.Libraries.Core.Assert;
using Nexus.Link.Libraries.Web.AspNet.Pipe.Inbound;

namespace Nexus.Link.Authentication.AspNet.Sdk.Logic
{
    public class ReentryAuthenticationService : IReentryAuthenticationService
    {
        private readonly IHashService _hashService;

        public ReentryAuthenticationService(IHashService hashService)
        {
            _hashService = hashService;
        }

        /// <inheritdoc />
        public async Task<bool> ValidateAsync(string reentryAuthentication, HttpContext context, CancellationToken cancellationToken = default)
        {
            InternalContract.RequireNotNullOrWhiteSpace(reentryAuthentication, nameof(reentryAuthentication));
            InternalContract.RequireNotNull(context, nameof(context));
            var token = RequestHelper.GetAuthorizationBearerTokenOrApiKey(new CompabilityInvocationContext(context));
            if (string.IsNullOrWhiteSpace(token)) return false;
            if (string.IsNullOrWhiteSpace(reentryAuthentication)) return false;
            var isValid = await _hashService.IsSameAsync(reentryAuthentication, token, cancellationToken);
            return isValid;
        }

        /// <inheritdoc />
        public Task<string> CreateAsync(HttpContext context, DateTimeOffset deleteAfter,CancellationToken cancellationToken = default)
        {
            InternalContract.RequireNotNull(context, nameof(context));
            var token = RequestHelper.GetAuthorizationBearerTokenOrApiKey(new CompabilityInvocationContext(context));
            if (string.IsNullOrWhiteSpace(token)) return null;
            return _hashService.CreateAsync(token, deleteAfter, cancellationToken);
        }
    }
}
#endif