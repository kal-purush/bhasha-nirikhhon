using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using RealEstateApp.Data;
using RealEstateApp.Models;

namespace RealEstateApp.Pages.Control_Panel
{
    public class CreateModel : PageModel
    {
        private readonly RealEstateApp.Data.ApplicationDbContext _context;
        private readonly ILogger<CreateModel> _logger;

        public CreateModel(RealEstateApp.Data.ApplicationDbContext context, ILogger<CreateModel> logger)
        {
            _context = context;
            _logger = logger;
        }

        public IActionResult OnGet()
        {
        ViewData["EstateTypeId"] = new SelectList(_context.EstateType, "EstateTypeId", "TypeName");
            return Page();
        }

        [BindProperty]
        public Estate Estate { get; set; } = default!;
        

        // To protect from overposting attacks, see https://aka.ms/RazorPagesCRUD
        public async Task<IActionResult> OnPostAsync()
        {
            if (!ModelState.IsValid)
            {
                _logger.LogInformation("Model state is invalid");
                foreach (var modelStateKey in ModelState.Keys)
                {
                    var modelStateVal = ModelState[modelStateKey];
                    foreach (var error in modelStateVal.Errors)
                    {
                        _logger.LogError($"Key: {modelStateKey}, Error: {error.ErrorMessage}");
                    }
                }
                return Page();
            }

            if (_context.Estate == null)
            {
                _logger.LogInformation("Database context's Estate DbSet is null");

                return Page();
            }

            if (Estate == null)
            {
                _logger.LogInformation("Estate object is null.");
                return Page();
            }
            if (!ModelState.IsValid || _context.Estate == null || Estate == null)
            {
                return Page();
            }

            _context.Estate.Add(Estate);
            await _context.SaveChangesAsync();
            _logger.LogInformation("OnPostAsync finished.");
            return RedirectToPage("./Index");
        }
    }
}