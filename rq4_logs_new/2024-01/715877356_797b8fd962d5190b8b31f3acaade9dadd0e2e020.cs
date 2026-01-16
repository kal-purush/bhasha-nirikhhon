using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using MyApp.CommonHelper;
using MyApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyApp.DataAccessLayer.DbInitalizer
{
    public class DbInitalizer : IDbInitalizer
    {
        private readonly UserManager<IdentityUser> _userManager;
        private readonly RoleManager<IdentityRole> _roleManager;
        private readonly ApplicationDbContext _context;

        public DbInitalizer(UserManager<IdentityUser> userManager, 
            RoleManager<IdentityRole> roleManager, 
            ApplicationDbContext context)
        {
            _userManager = userManager;
            _roleManager = roleManager;
            _context = context;
        }

        public void initalize()
        {
            try
            {
                if (_context.Database.GetPendingMigrations().Count() > 0)
                {
                    _context.Database.Migrate();
                }
                if (!_roleManager.RoleExistsAsync(WebsiteRoles.Role_Admin).GetAwaiter().GetResult())
                {
                    _roleManager.CreateAsync(new IdentityRole(WebsiteRoles.Role_Admin)).GetAwaiter().GetResult();
                    _roleManager.CreateAsync(new IdentityRole(WebsiteRoles.Role_User)).GetAwaiter().GetResult();
                    _roleManager.CreateAsync(new IdentityRole(WebsiteRoles.Role_Employee)).GetAwaiter().GetResult();
                    _userManager.CreateAsync(new ApplicationUser
                    {
                        UserName = "superadmin@gmail.com",
                        Email = "superadmin@gmail.com",
                        Name = "Super Admin",
                        City = "XYZ",
                        Address = "XYZ",
                        PinCode = 63000,
                        PhoneNumber = "0305000000",
                        State = "XYZ",
                    }, "Admin@123").GetAwaiter().GetResult();
                    ApplicationUser user = _context.ApplicationUsers.FirstOrDefault(x => x.UserName == "superadmin@gmail.com");
                    _userManager.AddToRoleAsync(user, WebsiteRoles.Role_Admin).GetAwaiter().GetResult();
                }
                return;
            }
            catch (Exception ex)
            {

            }
        }
    }
}