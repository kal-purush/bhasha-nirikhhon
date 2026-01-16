using Microsoft.AspNetCore.Mvc;

namespace Sidergin_web.Controllers
{
    public class ContactController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }
    }
}