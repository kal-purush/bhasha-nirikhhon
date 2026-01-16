using BookWebEcommerce.Data;
using BookWebEcommerce.Data.Static;
using BookWebEcommerce.Models;
using BookWebEcommerce.ViewModel;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace BookWebEcommerce.Controllers
{
	public class AccountController : Controller
	{
		private readonly UserManager<ApplicationUser> _userManager;
		private readonly SignInManager<ApplicationUser> _signInManager;
		private readonly AppDbContext _context;

		public AccountController(UserManager<ApplicationUser> userManager, SignInManager<ApplicationUser> signInManager, AppDbContext context)
		{
			_userManager = userManager;
			_signInManager = signInManager;
			_context = context;
		}

		public IActionResult Login()
		{
			return View(new LoginVM());
		}

		[HttpPost]
		public async Task<IActionResult> Login(LoginVM loginVM)
		{
			if (!ModelState.IsValid)
			{
				return View(loginVM);
			}
			var user = await _userManager.FindByEmailAsync(loginVM.EmailAddress);
			if(user != null)
			{
				var checkPassword = await _userManager.CheckPasswordAsync(user, loginVM.Password);
				if (checkPassword)
				{
					var result = await _signInManager.PasswordSignInAsync(user, loginVM.Password, false, false);
					if (result.Succeeded)
					{
                        return RedirectToAction("Index", "Book");
                    }
				}
                TempData["Error"] = "Please try again !!";
                return View(loginVM);
            }
            TempData["Error"] = "Wrong, Please try again !!";
            return View(loginVM);

        }
		public IActionResult Register() { return View(new RegisterVM()); }

		[HttpPost]
		public async Task<IActionResult> Register(RegisterVM registerVM)
		{
            if (!ModelState.IsValid) return View(registerVM);

            var user = await _userManager.FindByEmailAsync(registerVM.EmailAddress);
            if (user != null)
            {
                TempData["Error"] = "This email address is already in use";
                return View(registerVM);
            }

            var newUser = new ApplicationUser()
            {
                FullName = registerVM.FullName,
                Email = registerVM.EmailAddress,
                UserName = registerVM.FullName
            };
            var newUserResponse = await _userManager.CreateAsync(newUser, registerVM.Password);

            if (newUserResponse.Succeeded)
            {
                await _userManager.AddToRoleAsync(newUser, UserRoles.User);
            }
            else
            {
                foreach (var error in newUserResponse.Errors)
                {
                    ModelState.AddModelError(string.Empty, error.Description);
                }

                return View(registerVM);
            }

            return View("RegisterComplete");
        }

		[HttpPost]
		public async Task<IActionResult> Logout()
		{
            await _signInManager.SignOutAsync();
            return RedirectToAction("Index", "Book");
        }

		public async Task<IActionResult> User()
		{
			var items =await _context.Users.ToListAsync();
			return View(items);
		}

	}
}