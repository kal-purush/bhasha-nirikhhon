using System;
using System.Threading.Tasks;
using BrewFree.Data.Constants;
using BrewFree.Data.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace BrewFree
{
    public static class AspNetIdentityConfig
    {
        public static async Task UseAppAspNetIdentity(this IApplicationBuilder app)
        {
            using (var scope = app.ApplicationServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
            {
                var roleManager = scope.ServiceProvider.GetRequiredService<RoleManager<IdentityRole>>();
                var userManager = scope.ServiceProvider.GetRequiredService<UserManager<ApplicationUser>>();

                if (!await roleManager.RoleExistsAsync(RoleType.Braumeister))
                {
                    await roleManager.CreateAsync(new IdentityRole(RoleType.Braumeister));

                    var system = new ApplicationUser
                    {
                        Email = "support@brewfree.org",
                        UserName = "system",
                        Id = Guid.Empty.ToString()
                    };

                    await userManager.CreateAsync(system);
                }
            }
        }
    }
}