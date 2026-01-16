using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using WebAppProject.Data;
using WebAppProject.Models;
using Microsoft.EntityFrameworkCore;

namespace WebAppProject.Controllers
{
    public class LoginController : Controller
    {
        private readonly IHttpContextAccessor _httpContextAccessor;

        private readonly ApplicationDBContext _db;
  
        public LoginController(ApplicationDBContext db, IHttpContextAccessor httpContextAccessor) 
        {
            _db = db;
            _httpContextAccessor = httpContextAccessor;
        }
        [HttpPost("/test/{Id}")]
        public IActionResult GetUser(Guid Id)
        {
            var user = _db.UsersData.FirstOrDefault(u => u.Id == Id);

            return Ok(user);
        }
        public IActionResult Index()
        {
            return View();
        }
		public IActionResult SignIn()
		{
			return View();
		}
		[HttpPost]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> SignIn(UserData obj)
		{
            /*IEnumerable<UserData> allUser = _db.UsersData;*/
            /*var allUser = _db.UsersData.FromSql($"SELECT * FROM dbo.UsersData WHERE ").ToList();*/
            var propertyValue = obj.Email;

            var user = await _db.UsersData.FirstOrDefaultAsync(e => e.Email == propertyValue);

            Console.WriteLine(user.Email);


            if (obj.Email == user.Email && obj.Password == user.Password)
            {
                _httpContextAccessor.HttpContext.Session.SetString("Email", user.Email);

                return RedirectToAction("Index", "Party");
            }
            return RedirectToAction("Index");
        }
		public IActionResult SignUp()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult SignUp(UserData obj)
        {
            if (ModelState.IsValid)
            {
                _db.UsersData.Add(obj);
                _db.SaveChanges();
                _httpContextAccessor.HttpContext.Session.SetString("Email", obj.Email);
             
                return RedirectToAction("Index","Party");
            }
            return View(obj);
        }
    }
}