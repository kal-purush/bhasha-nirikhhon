using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace Milestone_cst_350.Controllers;

public class UserAuthorizationAttribute : Attribute, IAuthorizationFilter
{
    /// <summary>
    /// Prevents access when the Context is lacking a User session.
    /// </summary>
    /// <param name="context">AuthorizationFilterContext</param>
    public void OnAuthorization(AuthorizationFilterContext context)
    {
        // Reject no-user.
        string? username = context.HttpContext.Session.GetString("user");
        if (string.IsNullOrEmpty(username))
        {
            context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { action="Index", controller="Login"}));
        }
    }
}