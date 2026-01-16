using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;

namespace Colabora.WebAPI.Extensions;

public static class AuthExtensions
{
    private static void AddAuthenticationConfig(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddAuthentication(x =>
        {
            x.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
            x.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            x.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
        }).AddJwtBearer(x =>
        {
            x.TokenValidationParameters = new TokenValidationParameters
            {
                RequireAudience = true,
                ValidateIssuer = true,
                ValidateIssuerSigningKey = true,
                ValidateLifetime = true,
                ValidAudience = configuration.GetValue<string>("AuthenticationSettings:JwtAudience"),
                ValidIssuer = configuration.GetValue<string>("AuthenticationSettings:JwtIssuer"),
                IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(configuration.GetValue<string>("AuthenticationSettings:JwtKey")))
            };
        });
    }

    private static void AddAuthorizationPolicies(this IServiceCollection services)
    {
        services.AddAuthorization();
    }

    public static void AddSecuritySettings(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddAuthenticationConfig(configuration);
        services.AddAuthorizationPolicies();
    }
}