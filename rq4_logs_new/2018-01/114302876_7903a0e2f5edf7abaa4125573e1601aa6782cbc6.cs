using System.Threading.Tasks;
using proj4.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authorization.Infrastructure;

namespace proj4.Authorization {
    public class BandManagerAuthorizationHandler
                    : AuthorizationHandler<OperationAuthorizationRequirement, Band> {
        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context
            , OperationAuthorizationRequirement requirement
            , Band resource) {

            if (context.User == null) {
                return Task.FromResult(0);
            }

            if (context.User.IsInRole(Constants.BandManagerRole)) {
                context.Succeed(requirement);
            }

            return Task.FromResult(0);
        }
    }
}