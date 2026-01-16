using System.Web.Mvc;

namespace UI.Controllers
{
    public class RoutingController : Controller
    {
        [Route("{*route}")]
        public ActionResult Index(string route)
        {
            return View();
        }
    }
}