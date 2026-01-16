using Microsoft.AspNet.Mvc;

namespace BarneyPowell.Controllers
{
    public class HomeController : Controller
    {
        // GET: /<controller>/
        public IActionResult Index()
        {
            return View();
        }
    }
}