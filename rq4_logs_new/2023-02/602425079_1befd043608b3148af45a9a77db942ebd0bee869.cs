using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using StudentAttandanceLibrary.Repositories.IRepositories;

namespace StudentAttandance.Pages.User
{
    public class ViewAttendanceModel : PageModel
    {
        private ICourseRepository _courseRepository;
        private ITermRepository _termRepository;

        public ViewAttendanceModel(ICourseRepository courseRepository, ITermRepository termRepository)
        {
            _courseRepository = courseRepository;
            _termRepository = termRepository;
        }

        public void OnGet(int termId, int courseId)
        {
        }
    }
}