using Common.Settings;
using Microsoft.IdentityModel.Tokens;
using System.Security.Claims;

namespace Web.User.Helpers
{
    public class ViewHelper
    {
        public bool IsLoggedIn(ClaimsPrincipal principal)
        {
            return principal.Identity?.IsAuthenticated ?? false;
        }

        public string GetUserId(ClaimsPrincipal principal)
        {
            return principal.FindFirst(Constants.JwtIdKey)?.Value ?? string.Empty;
        }

        public string GetUsername(ClaimsPrincipal principal)
        {
            return principal.FindFirst(ClaimTypes.Name)?.Value ?? string.Empty;
        }

        public bool ShouldShowLoginRegisterLinks(HttpContext context)
        {
            var path = context.Request.Path.Value?.ToLowerInvariant();
            var canShowLogin = true;
            if (!string.IsNullOrEmpty(path))
            {
                canShowLogin = !(path.Contains("login") || path.Contains("register"));
            }
            return canShowLogin;
        }

        public RoleTypes GetUserRole(ClaimsPrincipal principal)
        {
            RoleTypes roleType = RoleTypes.AppUser;
            var role = principal.FindFirst(ClaimTypes.Role)?.Value ?? string.Empty;
            if(!string.IsNullOrEmpty(role))
            {
                switch (role)
                {
                    case RoleTypeNames.AppUser:
                        roleType = RoleTypes.AppUser;
                        break;
                    case RoleTypeNames.Partner:
                        roleType = RoleTypes.Partner;
                        break;
                    case RoleTypeNames.PartnerUser:
                        roleType = RoleTypes.PartnerUser;
                        break;
                    case RoleTypeNames.Administrator:
                        roleType = RoleTypes.Administrator;
                        break;
                }
            }
            return roleType;
        }
    }
}