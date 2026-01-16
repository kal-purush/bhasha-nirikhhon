using Examination.ViewModel;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Mvc;
using Mono.TextTemplating;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Security.Claims;
using Examination.Repo;
using Examination.Models;

namespace Examination.Controllers
{
    public class AccountController : Controller
    {
        IAccountRepo loginRepo;
        public AccountController(IAccountRepo _loginRepo)
        {
            loginRepo = _loginRepo;
        }
        [HttpGet]
        public IActionResult Login()
        {
            return View();
        }


		[HttpPost]
		public async Task<IActionResult> Login(LoginViewModel model)
		{
			if (!ModelState.IsValid)
			{
				return View(model);
			}

			var (user, userType) = loginRepo.GetUser(model);
			if (user == null&& userType==UserType.Unknown)
			{
				ModelState.AddModelError("", "Invalid username or password");
				return View(model);
			}

			Claim claim1;
			Claim claim2;
			ClaimsIdentity claimsIdentity = new ClaimsIdentity(CookieAuthenticationDefaults.AuthenticationScheme);

			switch (userType)
			{
				case UserType.Student:
					var student = (Student)user;
					claim1 = new Claim(ClaimTypes.Email, student.Semail);
					claim2 = new Claim(ClaimTypes.Name, student.Sname);
					claimsIdentity.AddClaim(new Claim(ClaimTypes.Role, "Student"));
					break;
				case UserType.Instructor:
					var instructor = (Instructor)user;
					claim1 = new Claim(ClaimTypes.Email, instructor.Insemail);
					claim2 = new Claim(ClaimTypes.Name, instructor.Insname);
					claimsIdentity.AddClaim(new Claim(ClaimTypes.Role, "Instructor"));
					break;
				case UserType.Admin:
					var admin = (Admin)user;
					claim1 = new Claim(ClaimTypes.Email, admin.Aemail);
					claim2 = new Claim(ClaimTypes.Name, admin.Aname);
					claimsIdentity.AddClaim(new Claim(ClaimTypes.Role, "Admin"));
					break;
				default:
					return RedirectToAction("Login", "Account");
			}

			claimsIdentity.AddClaim(claim1);
			claimsIdentity.AddClaim(claim2);

			ClaimsPrincipal claimsPrincipal = new ClaimsPrincipal(claimsIdentity);
			await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, claimsPrincipal);

			return RedirectToAction("Index", "Home");
		}


		public async Task <IActionResult> LogOut()

        {
            await HttpContext.SignOutAsync();
            return RedirectToAction("Login", "Account");
        }
        
    }
}