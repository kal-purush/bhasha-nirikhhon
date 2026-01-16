using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MVCswitchback.Models;

namespace MVCswitchback.Controllers
{
    public class TrailController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }


        static HttpClient client = new HttpClient();

        static void ShowTrails(Trail trail)
        {
            Console.WriteLine($"Name: {trail.name}");
        }

        static async Task<Uri> CreateTrailAsync(Trail trail)
        {
            HttpResponseMessage response = await client.PostAsJsonAsync("api/trail", trail);
            response.EnsureSuccessStatusCode();

            //return URI or the created resource.
            return response.Headers.Location;
        }
        static async Task<Trail> GetTrailAsync(string path)
        {
            Trail trail = null;
            HttpResponseMessage response = await client.GetAsync(path);
            if (response.IsSuccessStatusCode)
            {
                trail = await response.Content.ReadAsAsync<Trail>();
            }
            return trail;
        }
        static async Task<Trail> UpdateTrailAsync(Trail trail)
        {
            HttpResponseMessage response = await client.PutAsJsonAsync($"api/trails/{trail.id}", trail);
            response.EnsureSuccessStatusCode();

            //Deserialize the updatet trail from the response body
            trail = await response.Content.ReadAsAsync<Trail>();
            return trail;
        }
        static async Task<HttpStatusCode> DeleteTrailAsync(string id)
        {
            HttpResponseMessage response = await client.DeleteAsync($"api/trails/{id}");
            return response.StatusCode;
        }

    }
}