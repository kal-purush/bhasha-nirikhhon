using Microsoft.AspNetCore.Mvc;

namespace WebShop.Controllers
{
    public class ErrorController : Controller
    {
        public IActionResult index()
        {
            return View();
        }
    }
}