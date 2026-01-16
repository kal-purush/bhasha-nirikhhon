using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Darkec.Models;
using Darkec.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Darkec.Pages.Apartments
{
    public class GetApartmentModel : PageModel
    {
        [BindProperty]
        public Apartment Apartment { get; set; }

        private IObjectRepository<int, Apartment> repo;

        public GetApartmentModel(IObjectRepository<int, Apartment> repository)
        {
            repo = repository;
        }
        public IActionResult OnGet(int id)
        {
            Apartment = repo.GetObject(id);
            return Page();
        }
        public IActionResult OnPostBook(int id)
        {
            Apartment = repo.GetObject(id);
            repo.BookObject(LoginModel.CurrentUser, Apartment);
            return Page();
        }
        public IActionResult OnPostCancel(int id)
        {
            Apartment = repo.GetObject(id);
            repo.CancelObject(LoginModel.CurrentUser, Apartment);
            return Page();
        }
    }
}