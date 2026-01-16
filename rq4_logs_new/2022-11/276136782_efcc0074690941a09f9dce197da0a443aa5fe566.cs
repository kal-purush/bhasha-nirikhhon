using CautionaryAlertsApi.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using System.IdentityModel.Tokens.Jwt;

namespace CautionaryAlertsApi.Services
{
    public class UserResolverService : IUserResolverService
    {
        private readonly IHttpContextAccessor _context;

        public UserResolverService(IHttpContextAccessor context)
        {
            _context = context;
        }
        public string GetUserName()
        {
            return _context.HttpContext?.User.FindFirst(JwtRegisteredClaimNames.Sub).Value;
        }
    }
}