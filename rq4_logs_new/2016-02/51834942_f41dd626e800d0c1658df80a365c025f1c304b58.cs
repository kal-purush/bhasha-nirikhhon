using Microsoft.AspNet.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Wunderlist.Models;
using Wunderlist.Service;
using Wunderlist.Web.ViewModels;

namespace Wunderlist.Web.Controllers
{
    [Authorize]
    public class AccountController : Controller
    {

        private readonly IUserService userService;
        private readonly UserManager<AppUser,int> userManager;

        public AccountController(IUserService userService,UserManager<AppUser,int> userManager)
        {
            this.userService = userService;
            this.userManager = userManager;
        }

        [AllowAnonymous]
        // GET: Login
        public ActionResult Login()
        {
            return View();
        }

        // GET: Login
        [HttpPost]
        [AllowAnonymous]
        [ValidateAntiForgeryToken]
        public ActionResult Login(LoginViewModel viewModel)
        {
            if (ModelState.IsValid)
            {
                var user = userManager.Find(viewModel.Email, viewModel.Password);
                if (user != null)
                {
                    await SignInAsync(user);          
                    //TODO:  Change To Profile
                    return RedirectToAction("Index","Home");
                }
                else
                {
                    ModelState.AddModelError("", "Invalid username or password.");
                }
            }           
            return View(model);
        }
    }
}