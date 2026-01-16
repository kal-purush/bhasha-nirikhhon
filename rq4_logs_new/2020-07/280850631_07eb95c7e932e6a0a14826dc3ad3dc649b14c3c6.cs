using Covid19TemperatureAPI.Entities.Data;
using Covid19TemperatureAPI.Entities.Models;
using IdentityModel;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI
{
    public class SeedData
    {
        public static void EnsureSeedData(string connectionString)
        {
            var services = new ServiceCollection();
            services.AddDbContext<ApplicationDbContext>(options =>
               options.UseSqlServer(connectionString));

            services.AddIdentity<ApplicationUser, IdentityRole>()
                .AddEntityFrameworkStores<ApplicationDbContext>()
                .AddDefaultTokenProviders();

            using (var serviceProvider = services.BuildServiceProvider())
            {
                using (var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope())
                {
                    var context = scope.ServiceProvider.GetService<ApplicationDbContext>();
                    context.Database.Migrate();

                    #region Add 'admin' user 
                    var userMgr = scope.ServiceProvider.GetRequiredService<UserManager<ApplicationUser>>();
                    var admin = userMgr.FindByNameAsync("admin").Result;
                    if (admin == null)
                    {
                        admin = new ApplicationUser
                        {
                            UserName = "admin"
                        };
                        var result = userMgr.CreateAsync(admin, "Pass123$").Result;
                        if (!result.Succeeded)
                        {
                            throw new Exception(result.Errors.First().Description);
                        }

                        result = userMgr.AddClaimsAsync(admin, new Claim[]{
                        new Claim(JwtClaimTypes.Name, "covid19 admin"),
                        new Claim(JwtClaimTypes.GivenName, "admin"),
                        new Claim(JwtClaimTypes.FamilyName, ""),
                        new Claim(JwtClaimTypes.Email, "admin@admin.com"),
                        new Claim(JwtClaimTypes.EmailVerified, "true", ClaimValueTypes.Boolean),
                        new Claim(JwtClaimTypes.WebSite, ""),
                        new Claim(JwtClaimTypes.Address, @"{ 'street_address': '', 'locality': '', 'postal_code': , 'country': '' }", IdentityServer4.IdentityServerConstants.ClaimValueTypes.Json)
                    }).Result;
                        if (!result.Succeeded)
                        {
                            throw new Exception(result.Errors.First().Description);
                        }
                        Console.WriteLine("Admin created");

                    }
                    else
                    {
                        Console.WriteLine("Admin already exists");
                    }
                    #endregion

                    #region Add 'user' user
                    var user = userMgr.FindByNameAsync("orange").Result;
                    if (user == null)
                    {
                        user = new ApplicationUser
                        {
                            UserName = "orange"
                        };
                        var result = userMgr.CreateAsync(user, "Pass123$").Result;
                        if (!result.Succeeded)
                        {
                            throw new Exception(result.Errors.First().Description);
                        }

                        result = userMgr.AddClaimsAsync(user, new Claim[]{
                        new Claim(JwtClaimTypes.Name, "covid19 user"),
                        new Claim(JwtClaimTypes.GivenName, "orange"),
                        new Claim(JwtClaimTypes.FamilyName, ""),
                        new Claim(JwtClaimTypes.Email, "orange@orange.com"),
                        new Claim(JwtClaimTypes.EmailVerified, "true", ClaimValueTypes.Boolean),
                        new Claim(JwtClaimTypes.WebSite, ""),
                        new Claim(JwtClaimTypes.Address, @"{ 'street_address': '', 'locality': '', 'postal_code': , 'country': '' }", IdentityServer4.IdentityServerConstants.ClaimValueTypes.Json)
                    }).Result;
                        if (!result.Succeeded)
                        {
                            throw new Exception(result.Errors.First().Description);
                        }
                        Console.WriteLine("User created");
                    }
                    else
                    {
                        Console.WriteLine("User already exists");
                    }
                    #endregion

                }
            }
        }
    }
}