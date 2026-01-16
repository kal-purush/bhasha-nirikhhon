using Microsoft.AspNetCore.Identity;

namespace Algenic.Data.Initializers
{
    public static class IdentityUserInitializer
    {
        private static readonly string AdminEmail = "admin@admin";
        private static readonly string AdminPassword = "Paramount71Security";

        public static void SeedUsers(UserManager<IdentityUser> userManager)
        {
            var locatedUser = userManager.FindByEmailAsync(AdminEmail).Result;
            if (locatedUser != null)
            {
                return;
            }

            var user = new IdentityUser
            {
                UserName = AdminEmail,
                Email = AdminEmail
            };

            var result = userManager.CreateAsync(user, AdminPassword).Result;
            if (result.Succeeded)
            {
                userManager.AddToRoleAsync(user, "Admin").Wait();
            }
        }
    }
}