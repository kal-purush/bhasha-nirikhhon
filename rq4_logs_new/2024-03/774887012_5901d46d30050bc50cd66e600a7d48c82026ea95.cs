using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Collections.Generic;

namespace FYP_Management_System.Pages
{
    public class AdministratorHomePageModel : PageModel
    {
        public List<SupervisorRequest> SupervisorRequests { get; set; }
        public List<string> Fyps { get; set; }
        public List<string> Instructors { get; set; }

        public void OnGet()
        {
            // Fetch data from your database or other sources
            // Populate SupervisorRequests, Fyps, and Instructors lists
            // For demonstration purposes, I'll leave them as initialized lists

            SupervisorRequests = new List<SupervisorRequest>
            {
                new SupervisorRequest { SupervisorId = 1, SupervisorName = "Arshad Iqbal" },
                new SupervisorRequest { SupervisorId = 2, SupervisorName = "Idrees" },
                new SupervisorRequest { SupervisorId = 2, SupervisorName = "Sidra Khalid" }
            };

            Fyps = new List<string> { "Cross-Site Scripting (XSS) Prevention", "SQL Injection Prevention", "Firewall Rules Analyzer" };

            Instructors = new List<string> { "Shujan", "Hameed", "Ibrhaim" };
        }

        // Implement logic for approving supervisor request
        public IActionResult OnPostApproveRequest(int supervisorId)
        {
            // Your logic here
            return RedirectToPage("./Admin_homepage");
        }

        // Implement logic for rejecting supervisor request
        public IActionResult OnPostRejectRequest(int supervisorId)
        {
            // Your logic here
            return RedirectToPage("./Admin_homepage");
        }
    }

    public class SupervisorRequest
    {
        public int SupervisorId { get; set; }
        public string SupervisorName { get; set; }
    }
}