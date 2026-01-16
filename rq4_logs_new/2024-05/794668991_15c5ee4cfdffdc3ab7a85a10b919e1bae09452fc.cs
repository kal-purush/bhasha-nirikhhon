using Microsoft.AspNetCore.Mvc;
using TaskManagementSystem.Data;

namespace TaskManagementSystem.Controllers
{
    public class LoginController : Controller
    {
        private readonly TaskManagementDBContext _context;

        public LoginController(TaskManagementDBContext context)
        {
            _context = context;
        }

        public IActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Login(string username, string password)
        {
            var user = _context.Users.FirstOrDefault(u => u.UserName == username && u.Password == password);
            if (user != null)
            {
                return RedirectToAction("Dashboard", "TaskManagement", new { departmentId = user.DepartmentId });
            }
            else
            {
                ModelState.AddModelError("Invalid username or password", "Invalid username or password");
                return View();
            }
        }
    }
}