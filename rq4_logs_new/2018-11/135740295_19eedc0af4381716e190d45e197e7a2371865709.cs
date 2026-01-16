using System;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using movieGame.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;
using RestSharp;
using System.Collections.Generic;

namespace movieGame.Controllers.Game.MixedControllers
{
    public class GetMovieInfoController : Controller
    {
        private MovieContext _context;

        public GetMovieInfoController (MovieContext context)
        {
            _context = context;
        }


        public Movie GetAllMovieInfo(int movieId)
        {
            Movie movie = _context.Movies.Include (w => w.Clues).SingleOrDefault (x => x.MovieId == movieId);
            return movie;
        }


        // get all of a movie's JSON info based on movie title and release year
        public JObject GetMovieJSON (string MovieName, int MovieYear)
        {
            string apiQueryTitleYear = "https://www.omdbapi.com/?t=" + MovieName + "&y=" + MovieYear + "&apikey=4514dc2d";

            RestClient client = new RestClient (apiQueryTitleYear);
            RestRequest request = new RestRequest (Method.GET);
                request.AddHeader ("Postman-Token", "688d818e-29a5-498f-9c1c-907c7cb77c7f")
                        .AddHeader ("Cache-Control", "no-cache");

            IRestResponse response = client.Execute (request);
                string responseJSON = response.Content;

            JObject movieJObject = JObject.Parse (responseJSON);

            return movieJObject;
        }

        public int? GetMovieId(Movie movie)
        {
            int? movieId = movie.MovieId;
            return movieId;

        }
        public string GetMovieTitle(Movie movie)
        {
            string movieTitle = movie.Title;
            return movieTitle;
        }

        public string GetMovieTitle(JObject movieJObject)
        {
            string movieTitle = (string) movieJObject["Title"];
            return movieTitle;
        }

        public int? GetMovieReleaseYear(Movie movie)
        {
            int? movieReleaseYear = movie.Year;
            return movieReleaseYear;
        }

        public string GetMovieReleaseYear(JObject movieJObject)
        {
            string movieReleaseYear = (string) movieJObject["Year"];
            return movieReleaseYear;
        }

        public string GetMovieGenre(Movie movie)
        {
            string movieGenre = movie.Genre;
            return movieGenre;
        }

        public string GetMovieGenre(JObject movieJObject)
        {
            string movieGenre = (string) movieJObject["Genre"];
            return movieGenre;
        }

        public string GetMovieDirector(Movie movie)
        {
            string movieDirector = movie.Director;
            return movieDirector;
        }

        public string GetMovieDirector(JObject movieJObject)
        {
            string movieDirector = (string) movieJObject["Director"];
            return movieDirector;
        }

        public List<Clue> GetMovieClues(Movie movie)
        {
            List<Clue> clues = movie.Clues.ToList();
            return clues;
        }

        public Hints GetMoviesHints(Movie movie)
        {
            Hints thisMoviesHints = new Hints();
            thisMoviesHints.Director = GetMovieDirector(movie);
            thisMoviesHints.Genre = GetMovieGenre(movie);
            thisMoviesHints.ReleaseYear = GetMovieReleaseYear(movie).ToString();
            return thisMoviesHints;
        }

        public Hints GetMoviesHints(JObject movieJObject)
        {
            Hints thisMoviesHints = new Hints();
            thisMoviesHints.Director = GetMovieDirector(movieJObject);
            thisMoviesHints.Genre = GetMovieGenre(movieJObject);
            thisMoviesHints.ReleaseYear = GetMovieReleaseYear(movieJObject);

            return thisMoviesHints;
        }


        public void PrintHints(Hints h)
        {
            Console.WriteLine($"HINTS: Genre = {h.Genre}  |  ReleaseYear = {h.ReleaseYear}  |  Director = {h.Director}");
        }


        // public Hashtable GetMovieInfoHashTable (JObject movieJObject)
        // {
        //     string movieTitle = (string) movieJObject["Title"];
        //     string movieReleaseYear = (string) movieJObject["Year"];
        //     string movieGenre = (string) movieJObject["Genre"];
        //     string movieDirector = (string) movieJObject["Director"];

        //     // System.Collections.Hashtable
        //     Hashtable movieInfo = new Hashtable ();
        //     movieInfo.Add ("MovieTitle", movieTitle);
        //     movieInfo.Add ("MovieReleaseYear", movieReleaseYear);
        //     movieInfo.Add ("MovieGenre", movieGenre);
        //     movieInfo.Add ("MovieDirector", movieDirector);

        //     // set if you want the below to print to console
        //     bool executeWriteLines = false;

        //     if (executeWriteLines == true)
        //     {
        //         IDictionaryEnumerator _enumerator = movieInfo.GetEnumerator ();
        //         int _enumeratorCount = 1;
        //         while (_enumerator.MoveNext ())
        //         {
        //             Console.WriteLine ();
        //             Console.WriteLine (_enumeratorCount);
        //             Console.WriteLine ("Key   | " + _enumerator.Key.ToString ());
        //             Console.WriteLine ("Value | " + _enumerator.Value.ToString ());
        //             Console.WriteLine ();
        //             _enumeratorCount++;
        //         }
        //     };
        //     return movieInfo;
        // }



        [HttpGet]
        [Route ("Movie/{id}")]
        public IActionResult ShowMovie (int id)
        {

            #region DATABASE QUERIES
            ViewBag.Movies = _context.Movies.Include (w => w.Clues).SingleOrDefault (x => x.MovieId == id);

            // CURRENT MOVIE ---> movieGame.Models.Movie
            var currentMovie = _context.Movies.Include (w => w.Clues).SingleOrDefault (x => x.MovieId == id);

            // CURRENT MOVIE TITLE ---> the title of the movie pulled from the database
            var currentMovieTitle = currentMovie.Title;
            // CurrentMovieTitle.Intro("current movie title");

            // CURRENT MOVIE YEAR ---> the release year of the movie pulled from the database
            var currentMovieYear = currentMovie.Year;
            #endregion DATABASE QUERIES

            JObject movieJObject = GetMovieJSON (currentMovieTitle, currentMovieYear);
            // movieJObject.Intro("movie object");

            string movieTitle = movieJObject["Title"].ToString ();
            movieTitle.Intro ("getting profile for movie");
            string movieRating = (string) movieJObject["Rated"];
            string movieYear = (string) movieJObject["Year"];

            string movieActors = ViewBag.Actors = (string) movieJObject["Actors"];
            string movieWriter = ViewBag.MovieWriter = (string) movieJObject["Writer"];
            string movieDirector = ViewBag.MovieDirector = (string) movieJObject["Director"];
            string movieGenre = ViewBag.MovieGenre = (string) movieJObject["Genre"];
            string moviePoster = ViewBag.MoviePoster = (string) movieJObject["Poster"];

            return View ("SingleMovie");
            // return RedirectToAction("ViewSingleMovie", "ShowViews");
        }



        public JsonResult GetActorImage (string actorName)
        {
            #region API REQUEST INFO
            // var APIkey = "4cbdf8913d9628d339184a127d136d68";
            var personsName = actorName;
            var client = new RestClient ("https://api.themoviedb.org/3/search/person?api_key=1a1ef1aa4b51f19d38e4a7cb134a5699&language=en-US&query=" + personsName + "&page=1&include_adult=false");
            var request = new RestRequest (Method.GET);
            request.AddHeader ("Postman-Token", "dbfd1014-ebcb-4c79-80eb-1b0eac81a888");
            request.AddHeader ("Cache-Control", "no-cache");
            IRestResponse response = client.Execute (request);
            #endregion

            #region JSON
            var responseJSON = response.Content;
            JObject actorJSON = JObject.Parse (responseJSON);
            #endregion

            #region QUERIES
            actorName = ViewBag.ActorName = (string) actorJSON["results"][0]["name"];
            int actorId = ViewBag.ActorId = (int) actorJSON["results"][0]["id"];
            actorName.Intro ("actor name");
            // ActorId.Intro("actor id");

            string pictureBaseURL = "https://image.tmdb.org/t/p/w";

            // these are picture sizes
            int pictureSizeLarge = 400;
            int pictureSizeMedium = 300;
            int pictureSizeSmall = 200;
            int pictureSizeSmallest = 92;

            string actorPictureURL = (string) actorJSON["results"][0]["profile_path"];

            string actorPicLarge = ViewBag.ActorPicLarge = pictureBaseURL + pictureSizeLarge + actorPictureURL;
            string actorPicMedium = ViewBag.ActorPicMedium = pictureBaseURL + pictureSizeMedium + actorPictureURL;
            string actorPicSmall = ViewBag.ActorPicSmall = pictureBaseURL + pictureSizeSmall + actorPictureURL;
            string actorPicSmallest = ViewBag.ActorPicSmallest = pictureBaseURL + pictureSizeSmallest + actorPictureURL;

            actorPicSmallest.Intro ("smallest");
            #endregion

            return Json (actorJSON);
        }



        // public JsonResult GetBackgroundPosters ()
        // {
        //     // MOVIE DB PAGES --> set the number of api pages you want to query; 20 posters per page
        //     int movieDbPages = 2;

        //     // TOP MOVIE POSTERS ---> System.Collections.ArrayList
        //     ArrayList topMoviePosters = new ArrayList ();

        //     ArrayList allPosterURLs = new ArrayList ();

        //     for (var x = 1; x <= movieDbPages; x++)
        //     {
        //         var client = new RestClient ("https://api.themoviedb.org/3/movie/popular?api_key=1a1ef1aa4b51f19d38e4a7cb134a5699&language=en-US&page=" + x + "&region=us");

        //         var request = new RestRequest (Method.GET);

        //         request.AddHeader ("Postman-Token", "1348f187-6d98-4453-ad72-0f795b433479");
        //         request.AddHeader ("Cache-Control", "no-cache");

        //         // RESPONSE ---> 'RestSharp.RestResponse'
        //         IRestResponse response = client.Execute (request);

        //         var responseJSON = response.Content;

        //         // BACKGROUND JSON ---> all movie JSON presented more cleanly (i.e., it has been parsed)
        //         JObject backgroundJSON = JObject.Parse (responseJSON);

        //         // BACKGROUND COUNT --> 20 (i.e., the number of posters on each api page)
        //         var backgroundCount = backgroundJSON["results"].Count ();

        //         for (var i = 0; i <= backgroundCount - 1; i++)
        //         {
        //             // ONE POSTER ---> '/inVq3FRqcYIRl2la8iZikYYxFNR.jpg' etc.
        //             var onePoster = (string) backgroundJSON["results"][i]["poster_path"];

        //             // POSTER URL ---> e.g., https://image.tmdb.org/t/p/w200/wridRvGxDqGldhzAIh3IcZhHT5F.jpg
        //             var posterURL = "https://image.tmdb.org/t/p/w200" + onePoster;

        //             // ONE TITLE ---> 'Deadpool' OR 'Justice League' etc.
        //             var oneTitle = (string) backgroundJSON["results"][i]["title"];

        //             topMoviePosters.Add (onePoster);
        //             allPosterURLs.Add (posterURL);
        //         };
        //         ViewBag.TopMoviePosters = topMoviePosters;
        //     }
        //     return Json (topMoviePosters);
        // }
    }
}