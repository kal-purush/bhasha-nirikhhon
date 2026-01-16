using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AJS.Web.Infrastructure.Attributes
{
    public class CustomAuthorizationAttribute : Attribute, IAsyncAuthorizationFilter
    {
        private readonly string[] allowedRoles;

        public CustomAuthorizationAttribute(params string[] allowedRoles)
        {
            this.allowedRoles = allowedRoles;
        }

        public Task OnAuthorizationAsync(AuthorizationFilterContext context)
        {
            var userIsAuthenticated = context.HttpContext
                                             .User
                                             .Identity
                                             .IsAuthenticated;

            if (userIsAuthenticated)
            {
                for (int i = 0; i < allowedRoles.Length; i++)
                {
                    var isInRole = context.HttpContext
                                          .User
                                          .IsInRole(allowedRoles[i]);

                    if (isInRole)
                    {
                        return Task.CompletedTask;
                    }
                }
            }

            return Task.Run(() => context.Result = new RedirectToActionResult(ProjectConstants.DeniedActionName, ProjectConstants.HomeControllerName, new { Area = string.Empty }));
        }
    }
}