using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MyAPI.DTOs
{
    public class MovieDTO
    {
        public int Id { get; set; }

        public string Title { get; set; }

        public string Trailer { get; set; }

        public string Summary { get; set; }

        public DateTime ReleaseDate { get; set; }

        public bool InTheaters { get; set; }

        public string Poster { get; set; }

        public List<ActorMovieDTO> Actors { get; set; }
        public List<MovieTheaterDTO> MovieTheaters { get; set; }
        public List<GenreDTO> Genres { get; set; }
    }
}