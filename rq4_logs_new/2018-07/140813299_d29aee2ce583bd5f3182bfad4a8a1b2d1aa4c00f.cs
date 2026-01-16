using IdentityServer4.Models;
using IdentityServer4.Test;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;

namespace SwaggerUI.AuthServer
{
    public static class Config
    {
        internal static IEnumerable<Client> Clients()
        {
            yield return new Client
            {
                AllowAccessTokensViaBrowser = true,
                AllowedGrantTypes = GrantTypes.Implicit,
                AllowedScopes = new[] { "readAccess", "writeAccess" },
                ClientId = "test-id",
                ClientName = "test-app",
                ClientSecrets = new[] { new Secret("test-secret".Sha256()) },
                RedirectUris = new[] {
                    "http://localhost:5000/resource-server/swagger/oauth2-redirect.html", // Kestrel
                }
            };
        }

        internal static IEnumerable<ApiResource> ApiResources()
        {
            yield return new ApiResource
            {
                Name = "api",
                DisplayName = "API",
                Scopes = new[]
                {
                    new Scope("readAccess", "Access read operations"),
                    new Scope("writeAccess", "Access write operations")
                }
            };
        }

        internal static List<TestUser> TestUsers()
        {
            return new List<TestUser>
            {
                new TestUser
                {
                    SubjectId = "olanordmann",
                    Username = "olanordmann",
                    Password = "12345678"
                }
            };
        }


        internal static IServiceCollection AddOAuthAuthentication(this IServiceCollection services)
        {
            // Register IdentityServer services to power OAuth2.0 flows
            services.AddIdentityServer()
                .AddDeveloperSigningCredential()
                .AddInMemoryClients(Clients())
                .AddInMemoryApiResources(ApiResources())
                .AddTestUsers(TestUsers());

            // The auth setup is a little nuanced because this app provides the auth-server & the resource-server
            // Use the "Cookies" scheme by default & explicitly require "Bearer" in the resource-server controllers
            // See https://docs.microsoft.com/en-us/aspnet/core/security/authorization/limitingidentitybyscheme?tabs=aspnetcore2x
            services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
                .AddCookie()
                .AddIdentityServerAuthentication(c =>
                {
                    c.Authority = "http://localhost:5000/auth-server/";
                    c.RequireHttpsMetadata = false;
                    c.ApiName = "api";
                });

            // Configure named auth policies that map directly to OAuth2.0 scopes
            services.AddAuthorization(c =>
            {
                c.AddPolicy("readAccess", p => p.RequireClaim("scope", "readAccess"));
                c.AddPolicy("writeAccess", p => p.RequireClaim("scope", "writeAccess"));
            });

            return services;
        }
    }
}