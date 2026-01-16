using Microsoft.AspNetCore.Authorization;

namespace DotNetCoreWebHost.Authorization
{
    public static class AuthorizationPolicyBuilderExtensions
    {
        public static AuthorizationPolicyBuilder AddClaimTypeRequirement(
            this AuthorizationPolicyBuilder builder, 
            string claimType)
        {
            builder.AddRequirements(new RequireClaimType(claimType));

            return builder;
        }
    }
}