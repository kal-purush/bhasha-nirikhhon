using System;
using System.Security.Claims;
using System.Threading.Tasks;
using FCommon.Src.AccessToken;
using FCommon.Src.Constants;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;

namespace F5.Src.Presentation.Filters.Authorization;

public sealed class F5AuthorizationRequirementHandler
    : AuthorizationHandler<F5AuthorizationRequirement>
{
    private readonly Lazy<IHttpContextAccessor> _httpContextAccessor;

    public F5AuthorizationRequirementHandler(Lazy<IHttpContextAccessor> httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    protected override Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        F5AuthorizationRequirement requirement
    )
    {
        var expClaimValue = context.User.FindFirstValue(AppConstants.JsonWebToken.ClaimType.EXP);
        var isTokenExpired = AppAccessTokenHandler.IsAccessTokenExpired(expClaimValue);
        if (isTokenExpired)
        {
            context.Fail();

            return Task.CompletedTask;
        }

        var purposeClaimValue = context.User.FindFirstValue(
            AppConstants.JsonWebToken.ClaimType.PURPOSE.Name
        );
        if (
            !Equals(
                purposeClaimValue,
                AppConstants.JsonWebToken.ClaimType.PURPOSE.Value.USER_PASSWORD_RESET
            )
        )
        {
            context.Fail();

            return Task.CompletedTask;
        }

        var httpContext = _httpContextAccessor.Value.HttpContext;
        var stateBag = new F5StateBag
        {
            ResetPasswordTokenId = long.Parse(
                context.User.FindFirstValue(AppConstants.JsonWebToken.ClaimType.JTI)
            ),
            UserId = long.Parse(
                context.User.FindFirstValue(AppConstants.JsonWebToken.ClaimType.SUB)
            ),
        };
        httpContext.Items.Add(AppConstants.STATE_BAG_NAME, stateBag);

        context.Succeed(requirement);

        return Task.CompletedTask;
    }
}