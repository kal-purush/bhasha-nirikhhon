using IdeasRepository.DAL.Managers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.Owin;
using IdeasRepository.DAL.Entities;
using Microsoft.Owin.Security;
using System.Security.Claims;
using IdeasRepository.BL.Interfaces;

namespace IdeasRepository.BL.Providers
{
    public class AccountsProvider : IAccountsProvider
    {
        private HttpContextBase _context;

        public ApplicationUserManager UserManager
        {
            get
            {
                return _context.GetOwinContext().GetUserManager<ApplicationUserManager>();
            }
        }

        public ApplicationRoleManager RoleManager
        {
            get
            {
                return _context.GetOwinContext().GetUserManager<ApplicationRoleManager>();
            }
        }

        public IAuthenticationManager AuthManager
        {
            get
            {
                return _context.GetOwinContext().Authentication;
            }
        }

        public AccountsProvider(HttpContextBase context)
        {
            _context = context;
        }

        //public async void SignIn(ApplicationUser user)
        //{
        //    ClaimsIdentity claim = await UserManager.CreateIdentityAsync(user,
        //                                    DefaultAuthenticationTypes.ApplicationCookie);
        //    AuthManager.SignOut();
        //    AuthManager.SignIn(new AuthenticationProperties
        //    {
        //        IsPersistent = true
        //    }, claim);
        //}

        //public async Task<ApplicationUser> FindUserAsync(string userName, string userPassword)
        //{
        //    var user = await UserManager.FindAsync(userName, userPassword);
        //    return user;
        //}

        //public async Task<ApplicationUser> Find(string userName, string userPassword)
        //{
        //    var user = await UserManager.FindAsync(userName, userPassword);
        //    return user;
        //}

        //public async Task<IdentityResult> CreateUserAsync(ApplicationUser user, string userPassword)
        //{
        //    var result = await UserManager.CreateAsync(user, userPassword);

        //    return result;
        //}


        //public void SignOut()
        //{
        //    AuthManager.SignOut();
        //}


    }
}