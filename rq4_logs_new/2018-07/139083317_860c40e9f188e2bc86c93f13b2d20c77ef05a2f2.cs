using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace NoodleApp.Controllers
{
    public class NoodleController : Controller
    {
        public async Task<IActionResult> Index()
        {
            using (var client = new HttpClient())
            {
                // add the appropriate properties on top of the client base address.
                client.BaseAddress = new Uri("https://noodliciousapi.azurewebsites.net/");

                //the .Result is important for us to extract the result of the response from the call
                var response = client.GetAsync("/api/noodle").Result;

                if (response.EnsureSuccessStatusCode().IsSuccessStatusCode)
                {
                    var stringResult = await response.Content.ReadAsStringAsync();
                }
                return View();
            }
        }
    }
}