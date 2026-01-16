using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;

namespace assigMVCPeople.Controllers
{
    public class AdminController : Controller
    {
        private readonly RoleManager<IdentityRole> _roleManager;
        private readonly UserManager<AppUser> _userManager;
        public AdminController(RoleManager<IdentityRole> roleManager, UserManager<AppUser> userManager)
        {
            _roleManager=roleManager;
            _userManager=userManager;

        }

        public IActionResult Index()
        {
            return View();
        }
    }
}