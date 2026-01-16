using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using IdentityServer4.Extensions;
using IdentityServerAspNetIdentity.Models;
using IdentityServerHost.Quickstart.UI;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;

namespace IdentityServerAspNetIdentity.Quickstart.Profile
{
    [SecurityHeaders]
    [Authorize]
    public class ProfileController : Controller
    {
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly SignInManager<ApplicationUser> _signInManager;

        public ProfileController(
            UserManager<ApplicationUser> userManager,
            SignInManager<ApplicationUser> signInManager)
        {
            _userManager = userManager;
            _signInManager = signInManager;
        }

        [HttpGet]
        public IActionResult Index()
        {
            ApplicationUser user = _userManager.FindByIdAsync(User.GetSubjectId()).Result;

            ProfileUpdateViewModel vm = new ProfileUpdateViewModel { 
                DisplayName = user.DisplayName,
                Email = user.Email
            };

            return View("Index", vm);
        }


        /// <summary>
        /// Handle postback from username/password login
        /// </summary>
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Index(ProfileUpdateModel model, string button)
        {
            ApplicationUser user = _userManager.FindByIdAsync(User.GetSubjectId()).Result;

            ProfileUpdateViewModel vm = new ProfileUpdateViewModel
            {
                DisplayName = user.DisplayName,
                Email = user.Email,
                IsUpdated = true
            };

            if (ModelState.IsValid)
            {
                if (user != null)
                {
                    user.DisplayName = model.DisplayName;
                    await _userManager.UpdateAsync(user);
                    vm.IsUpdated = true;
                }
            }

            return View("Index", vm);
        }
    }
}