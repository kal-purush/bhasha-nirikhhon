using Microsoft.AspNetCore.Mvc;

namespace AppVecinos.Web.Controllers;

public class AccountController : Controller
{
    private readonly ILogger<HomeController> _logger;
    private readonly AuthService _authService;

    public AccountController(ILogger<HomeController> logger, AuthService authService)
    {
         _logger = logger;
        _authService = authService;
    }

    public IActionResult Login()
    {
        return View();
    }
    
    [HttpPost]
    public async Task<IActionResult> Login(string username, string password)
    {
        var token = await _authService.LoginAsync(username, password);

        if (token != null)
        {
            // Guardar el token en una cookie o en sesión
            HttpContext.Session.SetString("AuthToken", token);
            return RedirectToAction("Index", "Home");
        }

        // Mostrar mensaje de error si el login falla
        ViewBag.ErrorMessage = "Usuario o contraseña incorrectos.";
        return View();
    }
}