using MAJServices.Entities;
using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MAJServices.Seeds
{
    public class MyIdentityDataInitializer
    {
        public static void SeedData(UserManager<UserIdentity> userManager, RoleManager<RoleIdentity> roleManager)
        {
            seedUsersAsync(userManager);
            SeedRolesAsync(roleManager);
        }

        private static void seedUsersAsync(UserManager<UserIdentity> userManager)
        {
            
        }

        public static async void SeedRolesAsync(RoleManager<RoleIdentity> roleManager)
        {
            List<string> RolesList = new List<string>(new string[] { "SuperAdmin", "Admin", "Subadmin", "General" });
            foreach (string r in RolesList)
            {
                bool x = await roleManager.RoleExistsAsync(r);
                if (!x)
                {
                    //var role = new IdentityRole();
                    //role.Name = r;
                    var result =await roleManager.CreateAsync(new RoleIdentity { Name = r });
                }
            }
        }
    }
}