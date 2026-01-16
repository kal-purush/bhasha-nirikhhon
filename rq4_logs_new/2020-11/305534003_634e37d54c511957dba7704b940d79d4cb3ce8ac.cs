using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace _7024Movies.Pages
{
    public class MovieSearchTestModel : PageModel
    {
        public string MovieName;
        public SearchResults MovieSearchResults;
        static readonly HttpClient client = new HttpClient();

        public async Task OnGetAsync(string movieName)
        {
            if(string.IsNullOrEmpty(movieName)){
                return;
            }

            movieName.Replace(" ", "%20");
            string apiKey = "a852a3b3771672da86800503084b853b";
            //https://api.themoviedb.org/3/movie/550?api_key=a852a3b3771672da86800503084b853b
            //https://api.themoviedb.org/3/search/movie?api_key=a852a3b3771672da86800503084b853b&language=en-US&query=inception&page=1&include_adult=false

            // Call asynchronous network methods in a try/catch block to handle exceptions.
            try
            {
                string uri = String.Format("https://api.themoviedb.org/3/search/movie?api_key={0}&language=en-US&query={1}&page=1&include_adult=false", apiKey, movieName);
                HttpResponseMessage response = await client.GetAsync(uri);
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();
                // Above three lines can be replaced with new helper method below
                // string responseBody = await client.GetStringAsync(uri);
                MovieSearchResults = SearchResults.FromJson(responseBody);
            }
            catch
            {
                //TODO: display error
            }
        }
        public IActionResult OnGetNextPage(int id)
        {
            return Page();
        }
    }
}