using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using WebAdvertisements.Web.DTO;

namespace WebAdvertisement.Web.ServiceClients
{
    public class SearchApiClient : ISearchApiClient
    {
        private readonly HttpClient _client;
        private readonly string _baseAdress;

        public SearchApiClient(HttpClient client, IConfiguration configuration)
        {
            _client = client;
            _baseAdress = configuration.GetSection("SearchApi")["url"];
        }

        public async Task<List<AdvertType>> Search(string keyword)
        {
            var result = new List<AdvertType>();
            var callUrl = $"{_baseAdress}/api/v1/search/{keyword}";
            var httpResponse = await _client.GetAsync(new Uri(callUrl));

            if (httpResponse.StatusCode == HttpStatusCode.OK)
            {
                result = JsonConvert
                    .DeserializeObject<List<AdvertType>>(await httpResponse.Content.ReadAsStringAsync());
            }

            return result;
        }
    }
}