using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DotNetTeam7API.ViewModels
{
    public class MovieVM
    {

        public string Backdrop_path { get; set; }

        public string Name { get; set; }

        public string Overview { get; set; }

        public string Poster_path { get; set; }

        public double TMDB_average { get; set; }

        public double Korflix_average { get; set; }

        public MovieVM(
            string backdrop_path,
            string name,
            string overview,
            string poster_path,
            double tmdb_average,
            double korflix_average)
        {
            Backdrop_path = backdrop_path;
            Name = name;
            Overview = overview;
            Poster_path = poster_path;
            TMDB_average = tmdb_average;
            Korflix_average = korflix_average;
        }
    }
}