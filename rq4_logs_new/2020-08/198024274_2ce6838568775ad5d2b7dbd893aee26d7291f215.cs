using Nt.Domain.Entities.Movie;
using Nt.Domain.ServiceContracts.Movie;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nt.Application.Services.Movie
{
    public class MovieService : IMovieService
    {
        public Task<MovieEntity> CreateAsync(MovieEntity movie)
        {
            throw new NotImplementedException();
        }

        public Task<MovieEntity> GetOne(MovieEntity movie)
        {
            throw new NotImplementedException();
        }
    }
}