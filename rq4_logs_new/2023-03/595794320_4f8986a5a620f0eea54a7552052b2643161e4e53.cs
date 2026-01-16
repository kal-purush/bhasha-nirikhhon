using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using RomaF5patioComidas.Data;
using RomaF5patioComidas.Models;
using RomaF5patioComidas.Services.HomeService;
using System.Diagnostics;
using System.Security.Claims;

namespace RomaF5patioComidas.Controllers
{
    public class LoginController : Controller
    {
        
        private readonly IloginService _loginService;

        public LoginController(IloginService loginService)
        {            
            _loginService = loginService;
        }

        [HttpGet]
        public IActionResult Index()
        {          
            ClaimsPrincipal c = HttpContext.User;
            if (c.Identity != null)
            {
                if (c.Identity.IsAuthenticated)
                {
                    return RedirectToAction("index", "Mesas");
                }
            }
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Index([Bind("Usuario1,Clave")] Usuario usuario)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var user = await _loginService.verificarLog(usuario);

                    List<Claim> c = new List<Claim>()
                    {
                        new Claim(ClaimTypes.NameIdentifier, user.Usuario1),
                        new Claim(ClaimTypes.Role,user.IdCategoriaNavigation.Categoria)
                    };
                    ClaimsIdentity ci = new(c, CookieAuthenticationDefaults.AuthenticationScheme);
                    AuthenticationProperties p = new();
                    p.AllowRefresh = true;
                    p.IsPersistent = user.MantenerActivo;

                    if (!user.MantenerActivo)
                    {
                        p.ExpiresUtc = DateTimeOffset.UtcNow.AddMinutes(10);
                    }
                    else
                    {
                        p.ExpiresUtc = DateTimeOffset.UtcNow.AddDays(1);
                    }

                    await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(ci), p);                  

                }
                catch (Exception)
                {
                    ViewData["Mensaje"] = "usuario no encontrado";

                    return View(nameof(Index));
                }
                return RedirectToAction("index", "Mesas");

            }
            return View(usuario);

        }

        public async Task<IActionResult> Salir()
        {
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            return View(nameof(Index));
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}