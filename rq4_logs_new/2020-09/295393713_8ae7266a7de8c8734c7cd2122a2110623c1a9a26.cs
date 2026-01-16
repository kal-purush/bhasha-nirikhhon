using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

namespace SpaceParkFrontend.Pages
{
    public class ParkModel : PageModel
    {
        private readonly ILogger<ParkModel> _logger;

        public ParkModel(ILogger<ParkModel> logger)
        {
            _logger = logger;
        }

        public void OnGet()
        {
        }
    }
}