using System.Security.Claims;

namespace DatingApp.Extensions
{
    public static class ClaimPrincipalExtensions
    {
        public static string GetUsername(this ClaimsPrincipal user)
        {
            return user.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        }
    }
}