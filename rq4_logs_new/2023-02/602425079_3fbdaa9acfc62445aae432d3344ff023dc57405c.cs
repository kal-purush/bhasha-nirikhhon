using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace StudentAttandance.Pages.Admin
{
    public class LecturerManagementModel : PageModel
    {
        [BindProperty]
        public IFormFile ExcelFile { get; set; }

        public void OnGet()
        {
        }

        public IActionResult OnPostAdd()
        {
            // Form submission code here
            return RedirectToPage();
        }

        public IActionResult OnPostDelete()
        {
            // Form submission code here
            return RedirectToPage();
        }

        public IActionResult OnPostUpdate()
        {
            // Form submission code here
            return RedirectToPage();
        }
    }
}