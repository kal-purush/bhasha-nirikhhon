using MVCswitchback.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace MVCswitchback.Controllers
{
    public class BackendAPI
    {
        public static async Task<Rootobject> GetJObjectAsync(string SearchString)
        {
            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri("https://www.hikingproject.com");
                var response = client.GetAsync($"/data/get-trails?lat=40.0274&lon=-105.2519&maxDistance=10&key=200426075-bb9e04f2cd93ffc60dd2762d4f81ff2b").Result;
                response.EnsureSuccessStatusCode();

                var stringResult = await response.Content.ReadAsStringAsync();
                Rootobject rawTrail = JsonConvert.DeserializeObject<Rootobject>(stringResult);
                return rawTrail;
            }
        }
    }
}