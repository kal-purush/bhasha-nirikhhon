using System.Text;
using AtmosAQ.Infrastructure.Identity.Models;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;

namespace AtmosAQ.Web.Configurations
{
    public static class AuthenticationConfig
    {
        public static void SetupAuthentication(this IServiceCollection services, IConfiguration configuration)
        {
            JwtToken token = configuration.GetSection("JwtToken").Get<JwtToken>();

            services
                .AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                    options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
                    options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
                })
                .AddJwtBearer(options =>
                {
                    options.SaveToken = true;
                    options.RequireHttpsMetadata = false;
                    options.TokenValidationParameters = new TokenValidationParameters()
                    {
                        ValidateIssuer = true,
                        ValidIssuer = token.Issuer,

                        ValidateAudience = true,
                        ValidAudience = token.Audience,

                        RequireExpirationTime = true,
                        ValidateLifetime = true,

                        IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(token.Secret))
                    };
                })
                .AddIdentityServerJwt();
        }
    }
}