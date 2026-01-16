using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TwitterAPI.Application.Models.APIModels;
using TwitterAPI.Application.Services;
using TwitterAPI.Domain.Model;

namespace TwitterAPI.WebApp.Services
{
    public class DataService : IDataService
    {
        private readonly ILogger<DataService> _logger;
        private readonly HttpClient _httpClient;

        public DataService(
            HttpClient httpClient,
            ILogger<DataService> logger
            )
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<TweetAggregatedStatisticAPIModel> GetAggregatedStatistics()
        {
            try
            {
                var requestURI = "https://localhost:44313/Tweet/GetAggregatedStatistics";
                var response = await _httpClient.GetAsync(requestURI);
                var respondyContent = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<TweetAggregatedStatisticAPIModel>(respondyContent, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
                return result;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<List<TweetAPIModel>> GetTweets()
        {
            try
            {
                var requestURI = "https://localhost:44313/Tweet";
                var response = await _httpClient.GetAsync(requestURI);
                var respondyContent = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<List<TweetAPIModel>>(respondyContent, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
                return result;

            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
    }
}