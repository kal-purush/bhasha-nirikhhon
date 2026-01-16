using System.Net.NetworkInformation;
using System.Security.Claims;
using GameShopProject.Services;
using GameShopProject.ViewModels;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;

namespace GameShopProject.Controllers;

public class AccountController: Controller
{
    private readonly AccountService _service;

    public AccountController(AccountService service)
    {
        _service = service;
    }
    
    [HttpGet]
    public IActionResult Login()
    {
        return View();
    }
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Login(LoginModel model)
    {
        if (ModelState.IsValid)
        {
            var user = await _service.GetUserByEmailPassword(model.Email, model.Password);
            if (user != null)
            {
                await Authenticate(model.Email); // аутентификация
 
                return RedirectToAction("Index", "Home");
            }
            ModelState.AddModelError("", "Некорректные логин и(или) пароль");
        }
        return View(model);
    }
    private async Task Authenticate(string userName)
    {
        // создаем один claim
        var claims = new List<Claim>
        {
            new Claim(ClaimsIdentity.DefaultNameClaimType, userName)
        };
        // создаем объект ClaimsIdentity
        ClaimsIdentity id = new ClaimsIdentity(claims, "ApplicationCookie", ClaimsIdentity.DefaultNameClaimType, ClaimsIdentity.DefaultRoleClaimType);
        // установка аутентификационных куки
        await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(id));
    }
    
    [HttpGet]
    public IActionResult Register()
    {
        return View();
    }
    
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Register(RegisterModel model)
    {
        if (ModelState.IsValid)
        {
            var user = await _service.GetUserByEmail(model.Email);
            if (user == null)
            {
                // добавляем пользователя в бд
                await _service.AddUser(model.Email,model.Password, model.Name, model.Surname, model.DateOfBirthday);
                
                await Authenticate(model.Email); // аутентификация
 
                return RedirectToAction("Index", "Home");
            }
            else
                ModelState.AddModelError("", "Некорректные логин и(или) пароль");
        }
        return View(model);
    }
    
    public async Task<IActionResult> Logout()
    {
        await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        return RedirectToAction("Index", "Home");
    }

    public IActionResult Account()
    {
        var user = _service.GetUserByEmail(User.Identity.Name);

        if (user==null)
        {
            return RedirectToAction("Index", "Home");
        }

        return View(user);
    }
}