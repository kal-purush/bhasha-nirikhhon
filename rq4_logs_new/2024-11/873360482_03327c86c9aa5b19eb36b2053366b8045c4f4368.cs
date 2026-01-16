using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using HairSalon.Core.Entities;
using HairSalon.Infrastructure;

namespace HairSalon.Web.Pages.EmployeeSchedules
{
    public class CreateModel : PageModel
    {
        private readonly HairSalon.Infrastructure.HairSalonDbContext _context;

        public CreateModel(HairSalon.Infrastructure.HairSalonDbContext context)
        {
            _context = context;
        }

        [BindProperty]
        public EmployeeSchedule EmployeeSchedule { get; set; } = default!;

        public IActionResult OnGet()
        {
            return Page();
        }

        // For more information, see https://aka.ms/RazorPagesCRUD.
        public async Task<IActionResult> OnPostAsync()
        {
            if (!ModelState.IsValid)
            {
                return Page();
            }

            var employeeId = User.FindFirst("EmployeeId")?.Value;

            if (employeeId == null)
            {
                return RedirectToPage("/Login");
            }

            EmployeeSchedule.EmployeeId = int.Parse(employeeId);

            _context.EmployeeSchedules.Add(EmployeeSchedule);
            await _context.SaveChangesAsync();

            return RedirectToPage("./Index");
        }
    }
}