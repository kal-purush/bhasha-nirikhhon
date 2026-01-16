using System;
using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.EntityFramework;
using Microsoft.AspNet.Identity.Owin;
using Microsoft.Owin;

namespace LSCS.Web.Authentication
{
    public class LscsUserManager : UserManager<LscsUser>
    {
        public LscsUserManager(IUserStore<LscsUser> store) : base(store) { }

        public static LscsUserManager Create(IdentityFactoryOptions<LscsUserManager> options, IOwinContext context)
        {
            var manager = new LscsUserManager(new UserStore<LscsUser>(context.Get<UserDbContext>()));
            
            // Configure validation logic for usernames
            manager.UserValidator = new UserValidator<LscsUser>(manager)
            {
                AllowOnlyAlphanumericUserNames = true,
                RequireUniqueEmail = true
            };

            // Configure validation logic for passwords
            manager.PasswordValidator = new PasswordValidator
            {
                RequiredLength = 8,
                RequireNonLetterOrDigit = false,
                RequireDigit = false,
                RequireLowercase = false,
                RequireUppercase = false,
            };

            // Configure user lockout defaults
            manager.UserLockoutEnabledByDefault = true;
            manager.DefaultAccountLockoutTimeSpan = TimeSpan.FromMinutes(5);
            manager.MaxFailedAccessAttemptsBeforeLockout = 5;

       
            var dataProtectionProvider = options.DataProtectionProvider;
            if (dataProtectionProvider != null)
            {
                manager.UserTokenProvider =
                    new DataProtectorTokenProvider<LscsUser>(dataProtectionProvider.Create("ASP.NET Identity"));
            }
            return manager;
        }
    }
}