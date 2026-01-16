using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Darkec.Models;
using Darkec.Services;
using Microsoft.AspNetCore.Identity;


namespace Darkec.Pages
{
    public class RegisterModel : PageModel
    {
        [BindProperty]
        public User NewUser { get; set; }

        private IObjectRepository<int, User> repo;

        public RegisterModel(IObjectRepository<int, User> repository)
        {
            repo = repository;
        }

        public IActionResult OnGet()
        {
            return Page();
        }

        public IActionResult OnPost()
        {
            foreach (var user in repo.GetAllObjects().Values)
            {
                if (user.Email == NewUser.Email)
                {
                    return Page();
                }
            }
            repo.AddObject(NewUser);
            return RedirectToPage("Index");
        }
    }
}