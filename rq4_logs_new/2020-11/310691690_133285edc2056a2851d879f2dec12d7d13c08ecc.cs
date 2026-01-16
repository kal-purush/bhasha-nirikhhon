using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using GGsWeb.Models;
using GiantBomb.Api;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Newtonsoft.Json;

namespace GGsWeb.Controllers
{
    public class ManagerController : Controller
    {
        const string url = "https://localhost:44316/";
        private User user;
        const string apikey = "d77923d254b605206d54a4bec92d0f254ce238fc";

        public Encoding Enconding { get; private set; }

        public IActionResult GetInventory(int locationId)
        {
            user = HttpContext.Session.GetObject<User>("User");
            if (user == null)
                return RedirectToAction("Login", "Home");
            if (ModelState.IsValid)
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    // Get all locations for select list
                    var response = client.GetAsync("location/getAll");
                    response.Wait();

                    var result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        var jsonString = result.Content.ReadAsStringAsync();
                        jsonString.Wait();
                        var locations = JsonConvert.DeserializeObject<List<Location>>(jsonString.Result);
                        var locationOptions = new List<SelectListItem>();
                        foreach (var l in locations)
                        {
                            locationOptions.Add(new SelectListItem { Selected = false, Text = $"{l.city}, {l.state}", Value = l.id.ToString() });
                        }
                        ViewBag.locationOptions = locationOptions;
                    }

                    // Get inventory items at location
                    response = client.GetAsync($"inventoryitem/get/{locationId}");
                    response.Wait();

                    result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        var jsonString = result.Content.ReadAsStringAsync();
                        jsonString.Wait();

                        var model = JsonConvert.DeserializeObject<List<InventoryItem>>(jsonString.Result);
                        return View(model);
                    }
                }
            }
            return View();
        }
        [HttpGet]
        public IActionResult EditInventoryItem(int locationId, int videoGameId)
        {
            if (ModelState.IsValid)
            {
                // Get inventory item
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var response = client.GetAsync($"inventoryitem/get/{locationId}/{videoGameId}");
                    response.Wait();
                    var result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        var jsonString = result.Content.ReadAsStringAsync();
                        jsonString.Wait();

                        var model = JsonConvert.DeserializeObject<InventoryItem>(jsonString.Result);
                        return View(model);
                    }
                }
            }
            return View();
        }
        [HttpPost]
        public IActionResult EditInventoryItem(InventoryItem item)
        {
            user = HttpContext.Session.GetObject<User>("User");
            if (user == null)
                return RedirectToAction("Login", "Home");
            if (ModelState.IsValid)
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var json = JsonConvert.SerializeObject(item);
                    var data = new StringContent(json, Encoding.UTF8, "application/json");
                    var response = client.PutAsync("inventoryitem/update", data);
                    response.Wait();
                    var result = response.Result;
                    if (result.IsSuccessStatusCode)
                    {
                        // TODO: Give Confirmation 
                        return RedirectToAction("GetInventory", new { locationId = 1 });
                    }
                }
            }
            return RedirectToAction("GetInventory", new { locationId = 1 });
        }
        [HttpGet]
        public IActionResult AddInventoryItem(string searchString)
        {
            var giantBomb = new GiantBombRestClient(apikey);
            if (!String.IsNullOrEmpty(searchString))
            {
                var results = giantBomb.SearchForAllGames(searchString);
                return View(results);
            }
            return View(new List<GiantBomb.Api.Model.Game>());
        }
    }
}