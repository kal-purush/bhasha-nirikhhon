using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

namespace IS7024_Project.Pages
{
    public class CovidVaccineUserInputModel : PageModel
    {
        private readonly ILogger<CovidVaccineUserInputModel> _logger;

        [BindProperty]
        public UserInput userInput { get; set; }

        public CovidVaccineUserInputModel(ILogger<CovidVaccineUserInputModel> logger)
        {
            _logger = logger;
        }

        IList<UserInput> userInputs { get; set; }

        public void OnGet()
        {
            userInputs = UserInputRoster.allUserInputs;
            ViewData["UserInputList"] = userInputs;
        }

        public void OnPost()
        {
            string info = userInput.FirstName + userInput.LastName + userInput.Location;
            UserInputRoster.allUserInputs.Add(userInput);

            ViewData["UserInputList"] = UserInputRoster.allUserInputs;
        }
    }
}