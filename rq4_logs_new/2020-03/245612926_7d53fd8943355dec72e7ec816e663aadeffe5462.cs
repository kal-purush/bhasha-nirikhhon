using System;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Security.Principal;
using Microsoft.AspNetCore.Http;
using System.Security.Claims;

namespace CourseGenerator.Api.Middlewares
{
    public class ApiKeyMiddleware
    {
        private readonly RequestDelegate _next;

        public ApiKeyMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task Invoke(HttpContext context)
        {
            if (context.Request.Path.StartsWithSegments(new PathString("/api")))
            {
                if (context.Request.Headers.Keys.Contains("ApiKey", StringComparer.InvariantCultureIgnoreCase))
                {
                    string key = context.Request.Headers["ApiKey"].FirstOrDefault();
                    await ValidateKey(context, _next, key);
                }
                else
                {
                    context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                    await context.Response.WriteAsync("Api Key have to be specified");
                }
            }
            else
            {
                await _next(context);
            }
        }

        private async Task ValidateKey(HttpContext context, RequestDelegate next, string key)
        {
            bool valid = false;

            if (key == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                valid = true;

            if (valid)
            {
                var identity = new GenericIdentity("ShouldBeReplacedByApiKeyName");
                var principal = new GenericPrincipal(identity, new string[] { "User" });
                context.User = principal;

                await next(context);
            }
            else
            {
                context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                await context.Response.WriteAsync("Invalid Api Key");
            }
        }
    }
}