using Newtonsoft.Json;
using PieShop.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace PieShop.Core.Repository
{
    public class PieRepositoryWeb
    {
        private const string api = "http://192.168.101.156:5000/api/";

        public async Task<List<Pie>> GetAllPiesAsync()
        {
            HttpClient httpCliet= new HttpClient();
            httpCliet.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/jason"));
            var response = await httpCliet.GetAsync(api + "Pies");
            if (!response.IsSuccessStatusCode) return null;
            var jsonResult = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var pies = JsonConvert.DeserializeObject<IEnumerable<Pie>>(jsonResult);
            return pies.ToList();
        }
        public async Task<Pie> GetPieByIdAsync(int id)
        {
            HttpClient httpCliet = new HttpClient();
            httpCliet.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/jason"));
            var response = await httpCliet.GetAsync(api + "Pies/" + id);
            if (!response.IsSuccessStatusCode) return null;
            var jsonResult = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var pie = JsonConvert.DeserializeObject<Pie>(jsonResult);
            return pie;
        }
    }
}