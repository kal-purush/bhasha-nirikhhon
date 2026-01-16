using Microsoft.AspNetCore.Mvc;

namespace StoreAPI.Controllers
{
	public class CustomerController : Controller
	{
		public IActionResult Index()
		{
			return View();
		}
	}
}