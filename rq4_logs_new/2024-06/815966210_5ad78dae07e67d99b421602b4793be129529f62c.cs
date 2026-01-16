using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace SendIt.controllers
{
    public class Repository<T>
    {
        private readonly HttpClient _httpClient;

        public Repository(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public async Task<List<T>> GetAll(string endpoint)
        {
            HttpResponseMessage response = await _httpClient.GetAsync(endpoint);
            if (response.IsSuccessStatusCode)
            {
                string jsonString = await response.Content.ReadAsStringAsync();
                return JsonConvert.DeserializeObject<List<T>>(jsonString);
            }
            else
            {
                Console.WriteLine("Failed to retrieve data from API");
                return new List<T>();
            }
        }

        public async Task<bool> Add(string endpoint, T item)
        {
            string jsonContent = JsonConvert.SerializeObject(item);
            StringContent content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
            HttpResponseMessage response = await _httpClient.PostAsync(endpoint, content);
            return response.IsSuccessStatusCode;
        }
    }

}