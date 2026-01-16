using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DiscoverM;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding.Validation;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Serialization;
using IMDbApiLib;
using IMDbApiLib.Models;
using MovieId;

namespace _7024Movies.Pages
{
    public class DiscoverComedyModel : PageModel
    {
        public void OnGet()
        {
            using (var webClient = new WebClient())
            {
                string JsonString = webClient.DownloadString("https://api.themoviedb.org/3/discover/movie?api_key=a852a3b3771672da86800503084b853b&language=en-US&sort_by=popularity.desc&include_adult=false&include_video=false&page=1&vote_count.gte=3000&with_genres=35&with_original_language=en");
                JSchema schema = JSchema.Parse(System.IO.File.ReadAllText("DiscoverMovieSchema.json"));
                JObject jsonObject = JObject.Parse(JsonString);
                if (jsonObject.IsValid(schema))
                {
                    var discovercomedy = DiscoverM.DiscoverMovie.FromJson(JsonString);
                    var comedymovies = discovercomedy.Results;
                    ViewData["Comedies"] = comedymovies;
                    List<MovieId.MovieIds> comedyid = new List<MovieId.MovieIds>();
                    string uri = "";
                    foreach (DiscoverM.Result movie in comedymovies)
                    {
                        string movieid = movie.Id.ToString();
                        uri = String.Format("https://api.themoviedb.org/3/movie/{0}/external_ids?api_key=a852a3b3771672da86800503084b853b", movieid);
                        string JsonString2 = webClient.DownloadString(uri);
                        var movielookup = MovieId.MovieIds.FromJson(JsonString2);
                        comedyid.Add(movielookup);
                    }
                    ViewData["ComedyLkp"] = comedyid;
                    List<IMDB.MovieImdb> comedyimdb = new List<IMDB.MovieImdb>();
                    foreach (MovieIds movie in comedyid)
                    {
                        //k_3re1w44s
                        //k_vxcfqztc
                        string movieid = movie.ImdbId.ToString();
                        uri = String.Format("https://imdb-api.com/en/API/Title/k_vxcfqztc/{0}", movieid);
                        string ImdbString = webClient.DownloadString(uri);
                        var imdbrating = IMDB.MovieImdb.FromJson(ImdbString);
                        comedyimdb.Add(imdbrating);
                    }
                    ViewData["ComedyIMDB"] = comedyimdb;
                }
            }
        }
    }
}