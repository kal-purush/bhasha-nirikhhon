using DotNetTeam7API.Interfaces;
using DotNetTeam7API.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DotNetTeam7API.Services
{
    public class MovieService : IMovieService
    {
        private readonly IBaseRepository<Movie> _movieRepo;
        private readonly IBaseRepository<Genre> _genreRepo;

        public MovieService(IBaseRepository<Movie> movieRepo,
            IBaseRepository<Genre> genreRepo)
        {
            _movieRepo = movieRepo;
            _genreRepo = genreRepo;
        }
    }
}