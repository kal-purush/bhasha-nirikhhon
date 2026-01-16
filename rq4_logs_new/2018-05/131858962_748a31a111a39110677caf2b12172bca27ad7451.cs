using System.Collections.Generic;
using MovieApp.EntityModels;

namespace MovieApp
{
    public static class FakeDatabase
    {

        public static List<Movie> Movies = new List<Movie>()
        {
            new Movie() {
                Id = 1,
                Title = "Indiana Jones and the Last Crusade",
                ReleaseYear = 1989,
                Runtime = 127,
                Genre= "Adventure",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BMjNkMzc2N2QtNjVlNS00ZTk5LTg0MTgtODY2MDAwNTMwZjBjXkEyXkFqcGdeQXVyNDk3NzU2MTQ@._V1_SX300.jpg",
                Rating = 8.3
            },

            new Movie() {
                Id = 2,
                Title = "Star Wars: Episode IV - A New Hope",
                ReleaseYear = 1977,
                Runtime = 121,
                Genre= "Adventure",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BNzVlY2MwMjktM2E4OS00Y2Y3LWE3ZjctYzhkZGM3YzA1ZWM2XkEyXkFqcGdeQXVyNzkwMjQ5NzM@._V1_SX300.jpg",
                Rating = 8.6
            },

            new Movie() {
                Id = 3,
                Title =  "Guardians of the Galaxy",
                ReleaseYear = 2014,
                Runtime = 121,
                Genre= "Adventure",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BMTAwMjU5OTgxNjZeQTJeQWpwZ15BbWU4MDUxNDYxODEx._V1_SX300.jpg",
                Rating = 8.1
            },

            new Movie() {
                Id = 4,
                Title =  "Pulp Fiction",
                ReleaseYear = 1994,
                Runtime = 154,
                Genre= "Adventure",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BMTkxMTA5OTAzMl5BMl5BanBnXkFtZTgwNjA5MDc3NjE@._V1_SX300.jpg",
                Rating = 8.9
            },

            new Movie() {
                Id = 5,
                Title =  "Top Gun",
                ReleaseYear = 1986,
                Runtime = 110,
                Genre= "Action",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BZjQxYTA3ODItNzgxMy00N2Y2LWJlZGMtMTRlM2JkZjI1ZDhhXkEyXkFqcGdeQXVyNDk3NzU2MTQ@._V1_SX300.jpg",
                Rating = 6.9
            },

            new Movie() {
                Id = 6,
                Title = "The Fugitive",
                ReleaseYear = 1993,
                Runtime = 130,
                Genre= "Action",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BYmFmOGZjYTItYjY1ZS00OWRiLTk0NDgtMjQ5MzBkYWE2YWE0XkEyXkFqcGdeQXVyNjU0OTQ0OTY@._V1_SX300.jpg",
                Rating = 5.4
            },

            new Movie() {
                Id = 7,
                Title =  "Batman Forever",
                ReleaseYear = 1995,
                Runtime = 121,
                Genre= "Action",
                Image =  "https://images-na.ssl-images-amazon.com/images/M/MV5BNWY3M2I0YzItNzA1ZS00MzE3LThlYTEtMTg2YjNiOTYzODQ1XkEyXkFqcGdeQXVyMTQxNzMzNDI@._V1_SX300.jpg",
                Rating = 7.8
            },

            new Movie() {
                Id = 8,
                Title = "The Others",
                ReleaseYear = 2001,
                Runtime = 104,
                Genre= "Horror",
                Image = "https://images-na.ssl-images-amazon.com/images/M/MV5BMTAxMDE4Mzc3ODNeQTJeQWpwZ15BbWU4MDY2Mjg4MDcx._V1_SX300.jpg",
                Rating = 7.6
            },
        };

        public static List<Actor> Actors = new List<Actor>()
        {
            new Actor() { Id = 1, Name = "Harrison Ford", Age = 75 },
            new Actor() { Id = 2, Name = "Sean Connery", Age = 87 },
            new Actor() { Id = 3, Name = "Mark Hamill", Age = 66 },
            new Actor() { Id = 4, Name = "Carrie Fischer", Age = 60 },
            new Actor() { Id = 5, Name = "Chris Pratt", Age = 38 },
            new Actor() { Id = 6, Name = "Zoe Saldana", Age = 39 },
            new Actor() { Id = 7, Name = "John Travolta", Age = 64 },
            new Actor() { Id = 8, Name = "Tim Roth", Age = 56 },
            new Actor() { Id = 9, Name = "Samuel Jackson", Age = 69 },
            new Actor() { Id = 10, Name = "Uma Thurman", Age = 47 },
            new Actor() { Id = 11, Name = "Tom Cruise", Age = 55 },
            new Actor() { Id = 12, Name = "Val Kilmer", Age = 58 },
            new Actor() { Id = 13, Name = "Tommy Lee Jones", Age = 71 },
            new Actor() { Id = 14, Name = "Nicole Kidman", Age = 50 },
            new Actor() { Id = 15, Name = "Jim Carrey", Age = 56 },
            new Actor() { Id = 16, Name = "Julianne Moore", Age = 57 }
        };

        public static List<ActorMovieConnections> ActorsInMovies = new List<ActorMovieConnections>()
        {
            new ActorMovieConnections() { MovieId = 1, ActorId = 1 },
            new ActorMovieConnections() { MovieId = 1, ActorId = 2 },
            new ActorMovieConnections() { MovieId = 2, ActorId = 1 },
            new ActorMovieConnections() { MovieId = 2, ActorId = 3 },
            new ActorMovieConnections() { MovieId = 2, ActorId = 4 },
            new ActorMovieConnections() { MovieId = 3, ActorId = 5 },
            new ActorMovieConnections() { MovieId = 3, ActorId = 6 },
            new ActorMovieConnections() { MovieId = 4, ActorId = 7 },
            new ActorMovieConnections() { MovieId = 4, ActorId = 8 },
            new ActorMovieConnections() { MovieId = 4, ActorId = 9 },
            new ActorMovieConnections() { MovieId = 4, ActorId = 10 },
            new ActorMovieConnections() { MovieId = 5, ActorId = 11 },
            new ActorMovieConnections() { MovieId = 5, ActorId = 12 },
            new ActorMovieConnections() { MovieId = 6, ActorId = 1 },
            new ActorMovieConnections() { MovieId = 6, ActorId = 13 },
            new ActorMovieConnections() { MovieId = 6, ActorId = 16 },
            new ActorMovieConnections() { MovieId = 7, ActorId = 12 },
            new ActorMovieConnections() { MovieId = 7, ActorId = 14 },
            new ActorMovieConnections() { MovieId = 7, ActorId = 15 },
            new ActorMovieConnections() { MovieId = 8, ActorId = 14 }
        };
    }

}
