using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http.Headers;
using MovieFiles.Core.Models.People;
using MovieFiles.Core.Interfaces;

namespace MovieFiles.Adapters.Services
{
    public class PeopleServices : IPeopleService
    {
        private readonly HttpClient _httpClient;
        public PeopleServices(string apiToken)
        {
            _httpClient = new HttpClient();
            _httpClient.BaseAddress = new Uri("https://api.themoviedb.org/3/");
            _httpClient.DefaultRequestHeaders.Accept.Clear();
            _httpClient.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", apiToken);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        private async Task<T?> GetAsync<T>(string endpoint)
        {
            HttpResponseMessage response = await _httpClient.GetAsync(endpoint);
            if (!response.IsSuccessStatusCode)
            {
                return default(T);
            }

            string jsonResponse = await response.Content.ReadAsStringAsync();
            T? result = MovieApiUtil.ConvertApiMessage<T>(jsonResponse);
            return result;
        }

        public async Task<PeopleList?> GetPopularPeople(int page){
            return await GetAsync<PeopleList>($"person/popular?page={page}");
        }
    }
}