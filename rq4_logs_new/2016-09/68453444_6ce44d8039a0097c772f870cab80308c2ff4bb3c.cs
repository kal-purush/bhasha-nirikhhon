using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using TheWorld.Models;
using TheWorld.ViewModels;

namespace TheWorld.Controllers
{
   public class AuthController : Controller
   {
      private readonly SignInManager<WorldUser> _signInManager;

      public AuthController(SignInManager<WorldUser> signInManager)
      {
         _signInManager = signInManager;
      }

      public IActionResult Login()
      {
         if (User.Identity.IsAuthenticated)
         {
            return RedirectToAction("Trips", "App");
         }

         return View();
      }

      [HttpPost]
      public async Task<IActionResult> Login(LoginViewModel vm, string returnUrl)
      {
         if (ModelState.IsValid)
         {
            var signInResult = await _signInManager.PasswordSignInAsync(vm.Username, vm.Password, true, false);

            if (signInResult.Succeeded)
            {
               if (string.IsNullOrWhiteSpace(returnUrl))
               {
                  return RedirectToAction("Trips", "App");
               }
               return Redirect(returnUrl);
            }
            ModelState.AddModelError("", "Username or password incorrect");
         }

         return View();
      }

      public async Task<IActionResult> Logout()
      {
         if (User.Identity.IsAuthenticated)
         {
            await _signInManager.SignOutAsync();
         }

         return RedirectToAction("Index", "App");
      }
   }
}