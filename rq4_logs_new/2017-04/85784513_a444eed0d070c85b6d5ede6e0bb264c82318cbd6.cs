using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using ZenithWebsite.Models;
using System.Security.Claims;
using System.Security.Principal;

namespace ZenithWebsite.Controllers {
    [EnableCors("AllowAll")]
    [Produces("application/json")]
    [Route("api/RoleAPI")]
    public class RoleAPIController : Controller {
        private readonly ZenithContext _context;
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly SignInManager<ApplicationUser> _signInManager;


        public RoleAPIController(ZenithContext context, UserManager<ApplicationUser> userManager, SignInManager<ApplicationUser> signInManager) {
            _context = context;
            _userManager = userManager;
            _signInManager = signInManager;
        }

        public async Task<ClaimsIdentity> GetIdentity(string email, string password) {
            var result = await _signInManager.PasswordSignInAsync(email, password, false, lockoutOnFailure: false);
            if (result.Succeeded) {
                var user = await _userManager.FindByEmailAsync(email);
                var claims = await _userManager.GetClaimsAsync(user);

                return new ClaimsIdentity(new GenericIdentity(email, "Token"), claims);
            }

            // Credentials are invalid, or account doesn't exist
            return null;
        }

        // GET: api/RoleAPI
        [HttpGet]
        public async Task<ICollection<IdentityUserRole<string>>> GetRole() {
            var foo = HttpContext.User;
            var user = await _userManager.GetUserAsync(foo);
            var list = user.Roles;

            return list;
            //return _context.Event.Include(@e => @e.Activity);
        }

    }
}