using System.Threading.Tasks;
using Core.Entities.Identity;
using Microsoft.AspNetCore.Identity;

namespace Api.Extensions
{
    /// <summary>
    /// Is fired only if the application is first time running or identity table is empty
    /// </summary>
    public static class InitializeIdentityExtensions
    {
        #region Public Methods
        
        /// <summary>
        /// Execute methods to create first user with roles to using application
        /// </summary>
        public  static async Task FirstUsingApplication(
            RoleManager<IdentityRole> roleManager, 
            UserManager<AppUser> userManager)
        {
            await CreateRoles(roleManager);
            await CreateUser(userManager);
        }
        
        #endregion

        #region Private Methods
        
        /// <summary>
        /// Initialize AspNetRoles table in identity database with roles
        /// </summary>
        private static async Task CreateRoles(RoleManager<IdentityRole> roleManager)
        {
            IdentityRole[] roleIdentities =
            {
                new() {Name = "Admin"},
                new() {Name = "Point"}
            };

            foreach ( var roleIdentity in roleIdentities )
                await roleManager.CreateAsync ( roleIdentity );
        }

        /// <summary>
        /// Initialize AspNetUsers table in identity database with administrator user
        /// </summary>
        private static async Task CreateUser(UserManager<AppUser> userManager)
        {
            var appUser = new AppUser
            {
                UserName = "Admin"
            };
            
            await  userManager.CreateAsync ( appUser, "1w2q3e" );
            await userManager.AddToRoleAsync ( appUser, "Admin" );
        }
        
        #endregion
    }
}