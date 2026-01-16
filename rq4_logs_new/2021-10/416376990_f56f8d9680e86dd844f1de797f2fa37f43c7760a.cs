using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PopLibrary;

namespace PopApis.Controllers
{
    public class LoginController : Controller
    {
        private IAuthenticationService _authenticationService;

        public LoginController(IAuthenticationService authenticationService)
        {
            this._authenticationService = authenticationService;
        }
        public IActionResult Index()
        {
            return View();
        }

        public async Task<ActionResult> Login()
        {
            var userName = Request.Form["Input.UserName"];
            var password = Request.Form["Input.Password"];
            if (!string.IsNullOrWhiteSpace(userName) && !string.IsNullOrWhiteSpace(password))
            {
                var user = await this._authenticationService.AuthenticateAsync(userName, password).ConfigureAwait(false);
                if (user != null)
                {
                    HttpContext.Session.SetString("UserName", user.Name);
                    HttpContext.Session.SetString("UserRole", user.Role);
                    return RedirectToAction("Index", "Home");
                }
                else
                {
                    return View("Index");
                }
            }

            return View("Index");
        }

        public ActionResult Logout()
        {
            // clear session context
            HttpContext.Session.SetString("UserName", "");
            HttpContext.Session.SetString("UserRole", "");
            return RedirectToAction("Index", "Home");
        }
    }
}