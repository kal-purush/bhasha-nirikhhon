using Examination.Models;
using Examination.Repo;
using Microsoft.AspNetCore.Mvc;

namespace Examination.Controllers
{
	public class AdminDasbordController : Controller
	{
		AdminDashbordRepo Repo=new AdminDashbordRepo();
		public IActionResult Index()
		{
			ViewBag.stdCount=Repo.getStdsCount();
			ViewBag.insCount=Repo.getInsCount();
			ViewBag.crsCount=Repo.getCoursesCount();
			ViewBag.getTracks=Repo.getTracks();
			ViewBag.GetTracksCount = Repo.GetTracksCount();
            var viewModel = new ChartViewModel
            {
                Labels = Repo.getTracks().ToArray(),
                Data = Repo.GetTracksCount().ToArray()
            };
            return View(viewModel);
		}
	}
}