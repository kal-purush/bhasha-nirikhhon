using Examination.Repo;
using Microsoft.AspNetCore.Mvc;

namespace Examination.Controllers
{
	public class UpcommingExamsController : Controller
	{
		//this will be from auth data
		int StdId = 4;
		TeacherFormRepo repo = new TeacherFormRepo();
		public IActionResult Index()
		{
			var StdTrack = repo.GetTrackIdOfStudent(StdId);
			var StdCourses = repo.GetStudentCourses(StdId);
			List<int> coursesIds = new List<int>();
			foreach (var Courses in StdCourses)
			{
				coursesIds.Add(Courses.CrId);
			}
			var data = repo.HaveExam(StdTrack, coursesIds);
			foreach (var Courses in data)
			{
                Console.WriteLine(Courses);
            }
		    ViewBag.exam= data;
			return View();
		}
	}
}