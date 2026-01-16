using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using FalknerCountyJuvenileCourt.Data;
using FalknerCountyJuvenileCourt.Models;

namespace FalknerCountyJuvenileCourt.Pages.Crimes
{
    public class CreateModel : PageModel
    {
        private readonly FalknerCountyJuvenileCourt.Data.CourtContext _context;

        public CreateModel(FalknerCountyJuvenileCourt.Data.CourtContext context)
        {
            _context = context;
        }

        public IActionResult OnGet()
        {
        ViewData["FilingDecisionID"] = new SelectList(_context.FilingDecisions, "ID", "ID");
        ViewData["IntakeDecisionID"] = new SelectList(_context.IntakeDecisions, "ID", "ID");
        ViewData["JuvenileID"] = new SelectList(_context.Juveniles, "ID", "ID");
        ViewData["CrimeTypeID"] = new SelectList(_context.CrimeTypes, "ID", "Type");
        ViewData["SchoolID"] = new SelectList(_context.Schools, "ID", "Name");
            return Page();
        }

        [BindProperty]
        public Crime Crime { get; set; } = default!;
        

        // To protect from overposting attacks, see https://aka.ms/RazorPagesCRUD
        public async Task<IActionResult> OnPostAsync()
        {
          if (!ModelState.IsValid || _context.Crimes == null || Crime == null)
            {
                return Page();
            }

            _context.Crimes.Add(Crime);
            await _context.SaveChangesAsync();

            return RedirectToPage("./Index");
        }
    }
}