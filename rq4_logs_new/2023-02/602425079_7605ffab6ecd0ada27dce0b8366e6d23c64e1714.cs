using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using StudentAttandanceLibrary.Repositories.IRepositories;
using System.ComponentModel.DataAnnotations;

namespace StudentAttandance.Pages.Public
{
    public class ForgetPasswordModel : PageModel
    {
        private IMailRepository mailRepository;
        private ILogRepository logRepository;
        [Required(ErrorMessage = "Email is required")]
        [EmailAddress(ErrorMessage = "Invalid email address")]
        [BindProperty]
        public string Email { get; set; }
        public ForgetPasswordModel(IMailRepository mailRepository, ILogRepository logRepository)
        {
            this.mailRepository = mailRepository;
            this.logRepository = logRepository;
        }

        public void OnGet()
        {

        }

        public IActionResult OnPost()
        {
            if (ModelState.IsValid)
            {
                if (logRepository.ConfirmEmail(Email))
                {
                    mailRepository.sendNewPassword(Email);
                    ViewData["Message"] = "Open your email to get a new password!";
                }
                else
                {
                    ViewData["Message"] = "Your email is wrong!";
                }
            }
            return RedirectToPage("/public/login", new { message = ViewData["Message"] });
        }
    }
}