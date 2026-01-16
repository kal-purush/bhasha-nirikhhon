using GGsWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using GGsWeb.Models;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace GGsWeb.Controllers
{
    public class HomeController : Controller
    {
        const string url = "https://localhost:44316/";
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index(string email, string password)
        {
            ViewBag.email = email;
            ViewBag.password = password;
            return View();
        }
        public ViewResult Login()
        {
            return View();
        }
        [HttpPost]
        public IActionResult Login(User user)
        {
            if(ModelState.IsValid)
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var response = client.GetAsync($"user/get?email={user.email}");
                    response.Wait();

                    var result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        var jsonString = result.Content.ReadAsStringAsync();
                        jsonString.Wait();

                        var verifiedUser = JsonConvert.DeserializeObject<User>(jsonString.Result);

                        if (verifiedUser.password == user.password && verifiedUser.email == user.email)
                        {
                            //HttpContext.Session.Set<User>("CurrentUser", user);
                            return RedirectToAction("Index", "Customer");
                        }
                        else
                        {
                            return View(user);
                        }
                    }
                }
            }
            return View(user);
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}