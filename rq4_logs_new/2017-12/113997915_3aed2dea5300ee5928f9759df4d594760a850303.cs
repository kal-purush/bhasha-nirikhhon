using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using JoyOI.UserCenter.SDK;

namespace JoyOI.Monitor.Controllers
{
    public class AccountController : BaseController
    {
        [HttpGet]
        public IActionResult Login()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Login([FromServices] JoyOIUC UC, string username, string password)
        {
            var result = await UC.TrustedAuthorizeAsync(username, password);
            if (result.succeeded)
            {
                HttpContext.Session.SetInt32("login", 1);
                return RedirectToAction("Index", "Home");
            }
            else
            {
                return Prompt(x =>
                {
                    x.Title = "登录失败";
                    x.Details = result.msg;
                    x.StatusCode = 400;
                });
            }
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Logout()
        {
            HttpContext.Session.Remove("login");
            return RedirectToAction("Login", "Account");
        }
    }
}