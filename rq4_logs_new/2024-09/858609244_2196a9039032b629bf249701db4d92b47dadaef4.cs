using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace HandleLet.Pages
{
    public class IngredienserModel : PageModel
    {
        private readonly ILogger<IngredienserModel> _logger;

        public IngredienserModel(ILogger<IngredienserModel> logger)
        {
            _logger = logger;
        }

        public void OnGet()
        {
        }
    }
}