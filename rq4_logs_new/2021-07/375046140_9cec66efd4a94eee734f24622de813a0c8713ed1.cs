using System.Text;
using Core.Entities.Auth;
using Infrastructure.Identity;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;

namespace Api.Extensions
{
    public static class IdentityServiceExtensions
    {
        public static void AddIdentityService( this IServiceCollection services, IConfiguration config )
        {
            // Add identity adds cookie based authentication
            // Adds scoped classes for thinkgs like UserManager, SignInManager, PasswordHashers etc..
            // NOTE: Automatically adds the validated user from a cookie to the HttpContext.User
            services.AddIdentity<AppUser, IdentityRole>()

                // Adds UserStore and RoleStore from this context
                // That are consumed by the UserManager and RoleManager
                .AddEntityFrameworkStores<AppIdentityDbContext>();
            
                
            // Change password policy
            services.Configure<IdentityOptions> ( options =>
            {
                options.Password.RequireDigit = false;
                options.Password.RequiredLength = 5;
                options.Password.RequireLowercase = true;
                options.Password.RequireUppercase = false;
                options.Password.RequireNonAlphanumeric = false;
            } );
            
            
            services.AddAuthentication ( JwtBearerDefaults.AuthenticationScheme )
                .AddJwtBearer ( options =>
                {
                    options.TokenValidationParameters = new TokenValidationParameters
                    {
                        ValidateIssuerSigningKey = true,
                        IssuerSigningKey = new SymmetricSecurityKey ( Encoding.UTF8.GetBytes ( config["Token:Key"] ) ),
                        ValidIssuer = config["Token:Issuer"],
                        ValidateIssuer = true,
                        ValidateAudience = false
                    };
                } );
        }
    }
}