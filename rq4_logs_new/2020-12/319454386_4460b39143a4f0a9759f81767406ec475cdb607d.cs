using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetTeam7API.Models;
using Microsoft.EntityFrameworkCore;

namespace DotNetTeam7API.Data
{
    public class MovieDbSeeder
    {
        // tQ: return type for async is Task
        public static async Task SeedAsync(MovieDbSeeder db)
        {
            if (!await db.MovieGenres.AnyAsync())
            {
                // tQ: add range of all movies
            }
            if (!await db.Movies.AnyAsync())
            {
                // tQ: add range of all movies
            }
        }

        static IEnumerable<Movie> GetPreconfiguredItems()
        {
            return new List<Movie>()
        {

        };
        }

        static IEnumerable<Genre> GetPreconfiguredGenres()
        {

        }
    }
}