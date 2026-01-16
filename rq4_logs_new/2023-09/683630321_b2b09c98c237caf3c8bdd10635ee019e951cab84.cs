using Common.Exceptions;
using System.Security.Claims;

namespace Web.User.Helpers
{
    public class CacheKeyGenrator
    {
        public string CreateCacheKey(ClaimsPrincipal prinicpal, params string[] keys)
        {
            var userNameClaim = prinicpal.FindFirst(ClaimTypes.Name);
            if (userNameClaim == null)
            {
                throw new BusinessLogicException("Cannot create cache key.");
            }
            var userName = userNameClaim.Value;
            return $"{string.Join("_", keys)}_{userName}";
        }
    }
}