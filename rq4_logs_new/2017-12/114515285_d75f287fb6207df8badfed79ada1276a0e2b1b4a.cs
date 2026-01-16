using Microsoft.AspNetCore.Mvc;

namespace Tochka.Areas.Hr.Controllers
{
    [Area("Hr")]
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            return RedirectToAction("Index", "Vacancies", new { area = "Hr" });
        }
    }
}