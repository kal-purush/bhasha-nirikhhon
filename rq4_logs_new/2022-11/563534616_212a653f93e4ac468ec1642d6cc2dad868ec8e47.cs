using AutoMapper;
using DAL.Entities;
using DAL;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProgrammingForum_ASPNETCore.Models;
using System.Security.Claims;

namespace ProgrammingForum_ASPNETCore.Controllers
{
    public class AccountController : Controller
    {
        private readonly AppDbContext _context;
        private readonly IMapper _mapper;
        public AccountController(AppDbContext context, IMapper mapper)
        {
            _context = context;
            _mapper = mapper;
        }
        public IActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Login(string userName, string password, string ReturnUrl)
        {
            var user = _context.Users.Where(u => u.UserName == userName).FirstOrDefault();
            if (user != null) //if user exists in db + check password
            {
                var claims = new List<Claim>();
                claims.Add(new Claim(ClaimTypes.Name, userName));
                //claims.Add(new Claim(ClaimTypes.NameIdentifier, userName)); //id from db
                var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
                var claimsPrincipal = new ClaimsPrincipal(claimsIdentity);

                await HttpContext.SignInAsync(claimsPrincipal);
                if (!string.IsNullOrEmpty(ReturnUrl))
                {
                    return Redirect(ReturnUrl);
                }
                return Redirect("/");
            }
            TempData["Error"] = "Error: Username or Password is incorrect";
            return View("Login");
        }

        [Authorize]
        public async Task<IActionResult> Logout()
        {
            await HttpContext.SignOutAsync();
            return Redirect("/");
        }


        public IActionResult Register()
        {
            ViewBag.Message = "";
            return View();
        }

        [HttpPost]
        public IActionResult Register(UserCreateModel userCreate, IFormFile file)
        {
            if (file != null)
            {
                string extension = Path.GetExtension(file.FileName);
                List<string> extensions = new List<string>() { ".png", ".jpg", ".jpeg" };

                if (extensions.Contains(extension))
                {
                    using (var ms = new MemoryStream())
                    {
                        file.CopyTo(ms);
                        var fileBytes = ms.ToArray();
                        userCreate.Picture = fileBytes;
                    }
                }
                else
                {
                    ViewBag.Message = "Error: File extensions can be '.png', '.jpg', '.jpeg' ";
                    return View();
                }
            }

            //Add user to database if username, email, password correct/unique
            //if (!userCreate.Email.IsValidEmailAddress())
            //{
            //    ViewBag.Message = "Check your email";
            //    return View();
            //}
            //if (!userCreate.Password.IsValidPassword())
            //{
            //    ViewBag.Message = "Check your password"; // write limitation
            //    return View();
            //}

            // check if user with given email/password/username exists
            //
            var usermap = _mapper.Map<User>(userCreate);

            //_context.Users.Add(usermap);
            //_context.SaveChanges();


            return View("UserInfo", userCreate); // Return new view with user info; add edit
        }
    }
}