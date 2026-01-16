using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace orion.web.ApplicationStartup
{
    public class JwtWithCookieAuthMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly ILogger<JwtWithCookieAuthMiddleware> _logger;
        private static readonly string MehKey = Guid.NewGuid().ToString() + Guid.NewGuid().ToString() + DateTimeOffset.UtcNow.Ticks.ToString() + DateTime.Now.ToLongTimeString() + DateTime.Now.ToLongDateString() + Guid.NewGuid().ToString();
        public static readonly SymmetricSecurityKey securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(MehKey));
        public static readonly SigningCredentials Credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);
        public const string Issuer = "orion-web";
        private static HashSet<string> ExcludeFromAuthPaths = new HashSet<string>()
        {
            "/api/token"
        };

        public JwtWithCookieAuthMiddleware(RequestDelegate next, ILogger<JwtWithCookieAuthMiddleware> logger)
        {
            _next = next;
            _logger = logger;
        }


        public async Task InvokeAsync(HttpContext context)
        {
            if(context.Request.Path.StartsWithSegments("/api"))
            {
                if(ExcludeFromAuthPaths.Contains(context.Request.Path.ToString()) || await IsAuthorized(context))
                {

                    await _next(context);
                }
                else
                {
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    context.Response.Headers.Add("x-login-ep", "/api/token");
                    _logger.LogWarning("api call made without valid token!");
                }
            }
            else
            {
                // Call the next delegate/middleware in the pipeline
                await _next(context);
            }
        }
        private async Task<bool> IsAuthorized(HttpContext context)
        {
            if(context.Request.Headers.ContainsKey("Authorization"))
            {
                var auth = context.Request.Headers["Authorization"];
                if(auth.First().StartsWith("Bearer "))
                {
                    if(await SignInFromToken(context, auth.First()))
                    {
                        return true;
                    }
                }
            }
            return false;
        }
        public static readonly string key = Guid.NewGuid().ToString() + Guid.NewGuid().ToString();

        private async Task<bool> SignInFromToken(HttpContext context, string token)
        {
            try
            {
                var principal = new JwtSecurityTokenHandler().ValidateToken(token.Replace("Bearer ", ""), new TokenValidationParameters()
                {
                    IssuerSigningKey = securityKey,
                    ValidateAudience = false,
                    ValidIssuer = Issuer
                }, out var toke);

                var fullToken = toke as System.IdentityModel.Tokens.Jwt.JwtSecurityToken;

                var signInManager = context.RequestServices.GetService<SignInManager<IdentityUser>>();
                var user = await signInManager.UserManager.FindByNameAsync(fullToken.Subject);
                await signInManager.SignInAsync(user, true);
                return true;

            }
            catch(Exception e)
            {
                //eat it
                _logger.LogError(e, "Error trying to validate token");
            }
            return false;
        }

    }
}