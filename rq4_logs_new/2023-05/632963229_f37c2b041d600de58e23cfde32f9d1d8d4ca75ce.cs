using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LinqToDB;
using MovieFiles.Infrastructure.Mappers;

namespace MovieFiles.Infrastructure.Repositories
{
    public class MovieListRepository : BaseRepository
    {
        public MovieListRepository(string serverName, string databaseName, string userName, string password) : base(serverName, databaseName, userName, password){}

        public async Task addMovieToWatchLater(int movieId, Guid userId){
            MovieFiles.Core.Models.WatchLaterMovie movieList = new() {
                MovieId = movieId,
                UserId = userId,
                ListId = 0
            };
            using var db = GetQuantityDbUserConnection();
            var dbMovieList = DomToDb.Map(movieList);
            await db.InsertOrReplaceAsync(dbMovieList);
        }
    }
}