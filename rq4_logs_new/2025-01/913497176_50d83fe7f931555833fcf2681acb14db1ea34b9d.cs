using LibraryWebApp.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;

namespace LibraryWebApp.Helpers
{
    /// <summary>
    /// Seeds data
    /// </summary>
    public class DataSeeder
    {
        private string[] roles = { "User", "Admin" };

        /// <summary>
        /// Seeds roles into the database. If they already exists, it does nothing.
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <returns></returns>
        public async Task SeedRoles(IServiceProvider serviceProvider) 
        {
            using (var scope = serviceProvider.CreateScope())
            {
                var roleManager = scope.ServiceProvider.GetRequiredService<RoleManager<IdentityRole>>();

                // If even the first role exists, we assume that all roles exist and abort
                if (await roleManager.RoleExistsAsync(roles[0]))
                {
                    return;
                }

                foreach (var role in roles)
                {
                    await roleManager.CreateAsync(new IdentityRole(role));
                }
            }
        }

        /// <summary>
        /// Seeds users into the database. If they already exists, it does nothing
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <returns></returns>
        public async Task SeedUsers(IServiceProvider serviceProvider)
        {
            // Seed a normal user
            bool success = await SeedSingleUser("user", "user@gmail.com", "uuuuuuU!1", roles[0], serviceProvider);

            // Abort if the user was not seeded successfully
            if (success == false)
            {
                return;
            }

            // Seed an admin user
            await SeedSingleUser("admin", "admin@gmail.com", "aaaaaaA!1", roles[1], serviceProvider);
        }

        /// <summary>
        /// Seeds a single user into the database.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="email"></param>
        /// <param name="password"></param>
        /// <param name="role"></param>
        /// <param name="serviceProvider"></param>
        /// <returns>Whether the user was seeded successfully</returns>
        private async Task<bool> SeedSingleUser(string username, string email, string password, string role, IServiceProvider serviceProvider) 
        {
            using (var scope = serviceProvider.CreateScope())
            {
                var userManager = scope.ServiceProvider.GetRequiredService<UserManager<ApplicationUser>>();

                // If user already exists, we abort
                if (await userManager.FindByEmailAsync(email) != null)
                {
                    return false;
                }

                var user = new ApplicationUser
                {
                    UserName = username,
                    Email = email
                };

                var createUserResult = await userManager.CreateAsync(user, password);

                if (createUserResult.Succeeded)
                {
                    await userManager.AddToRoleAsync(user, role);
                }
            }

            return true;
        }
    }
}