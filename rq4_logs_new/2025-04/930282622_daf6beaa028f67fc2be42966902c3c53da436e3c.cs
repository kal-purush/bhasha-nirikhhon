using Bank.Application.Domain;
using Bank.UserService.Authorization;

using Microsoft.AspNetCore.Authorization;

namespace Bank.UserService.Security;

public static class PermissionPolicies
{
    public const string Admin           = "Permission.Admin";
    public const string Employee        = "Permission.Employee";
    public const string Client          = "Permission.Client";
    public const string AdminOrEmployee = "Permission.AdminOrEmployee";
    public const string AdminOrClient   = "Permission.AdminOrClient";
    public const string All             = "Permission.All";

    public static void AddPermissionPolicies(this AuthorizationOptions options)
    {
        options.AddPolicy(Admin,    policy => policy.Requirements.Add(new PermissionRequirement(new[] { Permission.Admin })));
        options.AddPolicy(Employee, policy => policy.Requirements.Add(new PermissionRequirement(new[] { Permission.Employee })));
        options.AddPolicy(Client,   policy => policy.Requirements.Add(new PermissionRequirement(new[] { Permission.Client })));

        options.AddPolicy(AdminOrEmployee, policy => policy.RequireAssertion(context =>
                                                                             {
                                                                                 var permissionClaim = context.User.FindFirst("permission");

                                                                                 if (permissionClaim != null && long.TryParse(permissionClaim.Value, out var value))
                                                                                 {
                                                                                     var permissions = new AuthPermission(value);

                                                                                     return permissions.HasPermission(Permission.Admin) ||
                                                                                            permissions.HasPermission(Permission.Employee);
                                                                                 }

                                                                                 return false;
                                                                             }));

        options.AddPolicy(AdminOrClient, policy => policy.RequireAssertion(context =>
                                                                           {
                                                                               var permissionClaim = context.User.FindFirst("permission");

                                                                               if (permissionClaim != null && long.TryParse(permissionClaim.Value, out var value))
                                                                               {
                                                                                   var permissions = new AuthPermission(value);

                                                                                   return permissions.HasPermission(Permission.Admin) ||
                                                                                          permissions.HasPermission(Permission.Client);
                                                                               }

                                                                               return false;
                                                                           }));

        options.AddPolicy(All, policy => policy.RequireAssertion(context =>
                                                                 {
                                                                     var permissionClaim = context.User.FindFirst("permission");

                                                                     if (permissionClaim != null && long.TryParse(permissionClaim.Value, out var value))
                                                                     {
                                                                         var permissions = new AuthPermission(value);

                                                                         return permissions.HasPermission(Permission.Admin) || permissions.HasPermission(Permission.Employee) ||
                                                                                permissions.HasPermission(Permission.Client);
                                                                     }

                                                                     return false;
                                                                 }));
    }
}