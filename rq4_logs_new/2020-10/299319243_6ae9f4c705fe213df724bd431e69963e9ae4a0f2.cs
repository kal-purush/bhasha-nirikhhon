using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CISS411_Project.Models;
using CISS411_Project.ViewModels;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;

namespace CISS411_Project.Controllers
{
    public class AccountController : Controller
    {
        private SwimDbContext db;
        private UserManager<RegisteredUser> userManager;
        private SignInManager<RegisteredUser> signInManager;
        private RoleManager<IdentityRole> roleManager;
        public AccountController(UserManager<RegisteredUser> userManager,
            SignInManager<RegisteredUser> signInManager,
            RoleManager<IdentityRole> roleManager, SwimDbContext db)
        {
            this.userManager = userManager;
            this.signInManager = signInManager;
            this.roleManager = roleManager;
            this.db = db;
        }
        public IActionResult Register()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Register(AccountRegisterViewModel vm)
        {
            if (ModelState.IsValid)
            {
                var user = new RegisteredUser { UserName = vm.Email, Email = vm.Email };
                var result = await userManager.CreateAsync(user, vm.Password);
                if (result.Succeeded)
                {
                    await signInManager.SignInAsync(user, false);
                    return RedirectToAction("Index", "Home");
                }
                else
                {
                    foreach (var error in result.Errors)
                    {
                        ModelState.AddModelError("", error.Description);
                    }
                }
            }
            return View(vm);
        }
    }
}