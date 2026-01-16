using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using YourScheduler.Infrastructure.Entities;

namespace YourScheduler.BusinessLogic.Models
{
    public class AuthorizationResponse
    {
        public int UserId { get; set; }

        public string DisplayName { get; set; } = null!;

        public string Email { get; set; } = null!;

        public string JwtAuth { get; set; } = null!;

        public AuthorizationResponse(ApplicationUser user, string token) 
        {
            UserId = user.Id;
            DisplayName = user.Displayname;
            Email = user.Email ?? throw new Exception("Email must be in database!");
            JwtAuth = token;
        }
    }
}