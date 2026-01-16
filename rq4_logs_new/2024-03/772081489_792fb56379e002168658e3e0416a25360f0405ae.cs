using Examination.Repo;
using Microsoft.AspNetCore.Mvc;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Examination.Controllers
{
	public class TeacherFormController : Controller
	{
		TeacherFormRepo repo=new TeacherFormRepo();
		public IActionResult Index()
		{
			int insId = 2;//will be from db
			ViewBag.TeacherCourses = repo.GetInsCourses(2);
			ViewBag.TeacherTracks=repo.GetInsTracks(2);
			return View();
		}
		[HttpPost]
		public IActionResult CreateExam(string CourseName, int TrackId, int NoOfMCQ, int NoOfTF, DateTime date, string start, string end, int insId)
        {
           //
            ViewBag.TeacherCourses = repo.GetInsCourses(2);
            ViewBag.TeacherTracks = repo.GetInsTracks(2);
            var res = repo.GenerateExam(CourseName, TrackId, NoOfMCQ, NoOfTF, date, start, end, insId);
			if (res == 1)
			{
                TempData["ErrorMessage"] = "Saved Correctly";
                TempData["ShowAlert"] = true;
                return RedirectToAction("confirmation", "teacherForm");
			}
			else
			{
                TempData["ErrorMessage"] = "Sorry, there is already an exam scheduled for this track and course on that day.";
                TempData["ShowAlert"] = true;
                return View("index");
			}

        }
		public IActionResult confirmation()
		{
			return  View();
		}
    }
}