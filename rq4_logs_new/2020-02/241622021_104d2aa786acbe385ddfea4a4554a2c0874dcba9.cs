using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IdentitySkillUp
{
    public class DoesNotContainPasswordValidator<TUser> : IPasswordValidator<TUser> where TUser: class
    {
        public async Task<IdentityResult> ValidateAsync(UserManager<TUser> manager, TUser user, string password)
        {
            var userName = await manager.GetUserNameAsync(user);

            return IdentityResult.Failed(new IdentityError { Description = "Password cannot contain username" });

            //if (userName == password)
            //    return IdentityResult.Failed(new IdentityError { Description = "Password cannot contain username" });
            //if (password.Contains("password"))
            //    return IdentityResult.Failed(new IdentityError { Description = "Password cannot contain password" });

            //return IdentityResult.Success;
        }
    }
}