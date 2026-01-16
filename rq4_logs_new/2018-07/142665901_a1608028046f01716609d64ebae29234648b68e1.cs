using System;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;

using AirporClientUWP.Models;
using System.Collections.ObjectModel;


namespace AirporClientUWP.Services
{
    class StewardessService
    {
        const string SERVER_NAME = "http://localhost:57338";

        public async Task<ObservableCollection<Stewardess>> GetAllAsync()
        {
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync(SERVER_NAME + "/api/stewardesses").ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    string contentResponse = await response.Content.ReadAsStringAsync();
                    return JsonConvert.DeserializeObject<ObservableCollection<Stewardess>>(contentResponse);
                }
            }

            throw new InvalidOperationException("Can`t get items");
        }

        public async Task<Pilot> GetAsync(Guid id)
        {
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync(SERVER_NAME + $"/api/stewardesses/{id}").ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    string contentResponse = await response.Content.ReadAsStringAsync();
                    return JsonConvert.DeserializeObject<Pilot>(contentResponse);
                }
            }

            throw new InvalidOperationException("Can`t get items");
        }

    }

}