using DotNetTeam7API.Data.TMDData;
using DotNetTeam7API.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace DotNetTeam7API.Data
{
    public class MovieDbSeedConverter
    {
        private static readonly HttpClient client = new HttpClient();

        public static List<Movie> movies = new List<Movie>();
        public static List<MovieGenre> genres = new List<MovieGenre>();

        public static async Task Convert()
        {
            movies.Clear();
            genres.Clear();

            using (var httpClient = new HttpClient())
            {
                using (var response = await httpClient.GetAsync("https://api.themoviedb.org/3/discover/tv?api_key=4d50e231ebab0b714167607ce53b71f1&language=en-US&with_original_language=ko"))
                {
                    string apiResponse = await response.Content.ReadAsStringAsync();

                    Root apiObjects = JsonConvert.DeserializeObject<Root>(apiResponse);

                    foreach(var res in apiObjects.results)
                    {
                        movies.Add(new Movie());
                            
                            {
                                Backdrop_path = res.backdrop_path,
                                First_air_date = res.first_air_date
                            }
                        );  
                    }
                }
            }
        }
    }
}