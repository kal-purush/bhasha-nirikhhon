using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using GGsWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace GGsWeb.Controllers
{
    public class VideoGameController : Controller
    {
        const string url = "https://localhost:44316/";
        public IActionResult Details(int id)
        {
            if (ModelState.IsValid)
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var response = client.GetAsync($"videogame/get?id={id}");
                    response.Wait();

                    var result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        var jsonString = result.Content.ReadAsStringAsync();
                        jsonString.Wait();

                        var model = JsonConvert.DeserializeObject<VideoGame>(jsonString.Result);
                        return View(model);
                    }
                }
            }
            return View();
        }
    }
}