using Bookworm_Society_API.Models;

namespace Bookworm_Society_API.Data
{
    public class GenreData
    {
        public static List<Genre> Genres = new()
       {
            new() { Id = 1, Name = "Historical Fiction" },
            new() { Id = 2, Name = "Fantasy" },
            new() { Id = 3, Name = "Science Fiction" },
            new() { Id = 4, Name = "Mystery" },
            new() { Id = 5, Name = "Romance" },
            new() { Id = 6, Name = "Thriller" },
            new() { Id = 7, Name = "Horror" },
            new() { Id = 8, Name = "Slice of Life" },
            new() { Id = 9, Name = "Drama" },
            new() { Id = 10, Name = "Adventure" }
       };
    }
}