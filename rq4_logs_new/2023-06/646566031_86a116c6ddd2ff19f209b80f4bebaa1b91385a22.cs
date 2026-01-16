using back_courrier.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace back_courrier.Pages
{
    [Authorize]
    public class DashboardModel : PageModel
    {
        private readonly Data.ApplicationDbContext _context;
        private readonly ICourrierService _courrierService;

        public Dictionary<string, int> CourriersByFlag { get; set; }

        public void OnGet()
        {
            CourriersByFlag = _courrierService.GetStatCourrierFlag();
        }

        public DashboardModel(Data.ApplicationDbContext context, ICourrierService courrierService)
        {
            _context = context;
            _courrierService = courrierService;
        }
    }
}