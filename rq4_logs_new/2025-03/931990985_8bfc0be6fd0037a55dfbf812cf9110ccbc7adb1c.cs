using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using UserService.DataAccess.Models;

namespace UserService.DataAccess.Data
{
    public class DatabaseInitializer
    {
        public static async Task InitializeAsync(IServiceProvider serviceProvider)
        {
            using var scope = serviceProvider.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

            await context.Database.MigrateAsync();

            await SeedRolesAsync(serviceProvider);
            await SeedUsersAsync(serviceProvider);
        }

        public static async Task SeedRolesAsync(IServiceProvider serviceProvider)
        {
            var roleManager = serviceProvider.GetRequiredService<RoleManager<IdentityRole<Guid>>>();

            string[] roles = { "Admin", "Client", "Courier" };
            foreach (var role in roles)
            {
                if (!await roleManager.Roles.AnyAsync(r => r.Name == role))
                {
                    await roleManager.CreateAsync(new IdentityRole<Guid>(role));
                }
            }
        }

        public static async Task SeedUsersAsync(IServiceProvider serviceProvider)
        {
            var userManager = serviceProvider.GetRequiredService<UserManager<ApplicationUser>>();

            var users = new[]
            {
                new { Name = "Splinter", Email = "admin@example.com", Password = "Admin123!", Role = "Admin" },
                new { Name = "Leonardo", Email = "client1@example.com", Password = "Client123!", Role = "Client" },
                new { Name = "Donatello", Email = "client2@example.com", Password = "Client123!", Role = "Client" },
                new { Name = "Michelangelo",  Email = "courier1@example.com", Password = "Courier123!", Role = "Courier" },
                new { Name = "Raphael",  Email = "courier2@example.com", Password = "Courier123!", Role = "Courier" }
            };

            foreach (var userData in users)
            {
                if (await userManager.FindByEmailAsync(userData.Email) is null)
                {
                    var user = new ApplicationUser
                    {
                        Name = userData.Name,
                        UserName = userData.Email,
                        Email = userData.Email,
                        EmailConfirmed = true
                    };

                    var result = await userManager.CreateAsync(user, userData.Password);

                    if (result.Succeeded)
                    {
                        await userManager.AddToRoleAsync(user, userData.Role);
                    }
                }
            }
        }
    }
}