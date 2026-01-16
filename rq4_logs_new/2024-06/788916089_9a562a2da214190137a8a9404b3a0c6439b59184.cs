using BookCatalog.Core.Interfaces;
using System.Text.RegularExpressions;

namespace BookCatalog.Infrastructure.Services
{
    public class PageRetrievalService : IPageRetrievalService
    {
        private readonly HttpClient _httpClient;

        public PageRetrievalService()
        {
            _httpClient = new HttpClient();
        }

        public async Task<int> GetPageCountAsync(string identifier)
        {
            var url = $"https://archive.org/details/{identifier}";

            try
            {
                var htmlContent = await _httpClient.GetStringAsync(url);
                var match = Regex.Match(htmlContent, @"<span itemprop=""numberOfPages"">(\d+)</span>");

                if (match.Success && int.TryParse(match.Groups[1].Value, out int pages))
                {
                    return pages;
                }
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine(ex.Message);
            }

            return 0;
        }
    }
}