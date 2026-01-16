using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace Algenic.Data.Initializers
{
    public static class IdentityRoleInitializer
    {
        public static void ApplyDefaultRoles(this ModelBuilder builder)
        {
            builder.Entity<IdentityRole>().HasData(
                new IdentityRole { Name = "Admin", NormalizedName = "Admin".ToUpper() },
                new IdentityRole { Name = "Examiner", NormalizedName = "Examiner".ToUpper() },
                new IdentityRole { Name = "Regular", NormalizedName = "Regular".ToUpper() });
        }
    }
}