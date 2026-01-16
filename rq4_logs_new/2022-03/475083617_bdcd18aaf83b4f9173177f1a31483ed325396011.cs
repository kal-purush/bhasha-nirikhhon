using AdvancedCSharpFinalProject.Data;
using AdvancedCSharpFinalProject.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace AdvancedCSharpFinalProject.Controllers
{
    public class MainController : Controller
    {
        private ApplicationDbContext _db { get; set; }
        private UserManager<ApplicationUser> _userManager { get; set; }
        private RoleManager<IdentityRole> _roleManager { get; set; }

        public MainController(ApplicationDbContext Db, UserManager<ApplicationUser> userManager, RoleManager<IdentityRole> roleManager)
        {
            _db = Db;
            _userManager = userManager;
            _roleManager = roleManager;
        }
        public IActionResult Index()
        {
            return View();
        }
        [Authorize]
        public IActionResult GetAllRolesForAUser(string? userId)
        {
            ViewBag.usersList = new SelectList(_db.Users.ToList(), "Id", "UserName");
            UserManager usm = new UserManager(_db);
            List<string> roleNamesOfUser = usm.GetAllRolesOfUser(userId);
            if(usm.User != null)
            {
                ViewBag.UserName = usm.User.UserName;
            }
            return View(roleNamesOfUser);
        }
    }
}