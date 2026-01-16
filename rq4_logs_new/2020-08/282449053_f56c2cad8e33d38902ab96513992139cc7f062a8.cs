using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace TodoApi.Infra.Extensions
{
    internal static class JwtContainer
    {
        public static IServiceCollection AddJwtAuthentication(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddAuthentication(options =>
                    {
                        options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                        options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
                    })
            .AddJwtBearer(options =>
                        {
                            options.Authority = configuration["Jwt:Authority"];
                            options.Audience = configuration["Jwt:Audience"];
                            //options.MetadataAddress = configuration["Jwt:MetadataAddress"];
                            options.RequireHttpsMetadata = false;
                        });
            return services;
        }


        public static IServiceCollection AddJwtAuthorization(this IServiceCollection services)
        {
            services.AddAuthorization(auth =>
                    {
                    auth.AddPolicy("Bearer", new AuthorizationPolicyBuilder()
                            .AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme)
                            .RequireAuthenticatedUser().Build());
                    });

            return services;
        }
    }
}
