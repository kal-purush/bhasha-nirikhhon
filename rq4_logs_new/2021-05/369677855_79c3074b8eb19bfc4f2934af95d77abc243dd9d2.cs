using System.Linq;
using System.Threading.Tasks;
using AtmosAQ.Infrastructure.Identity.Models;
using Microsoft.AspNetCore.Identity;

namespace AtmosAQ.Infrastructure.Persistence
{
    public static class DatabaseSeed
    {
        public static async Task SeedDefaultUSerAsync(UserManager<ApplicationUser> userManager)
        {
            var user = new ApplicationUser
            {
                UserName = "rick",
                Email = "rick@earth.com",
                EmailConfirmed = true
            };

            if (userManager.Users.All(x => x.UserName != user.UserName))
            {
                await userManager.CreateAsync(user, "!Sanchez1");
            }
        }
    }
}