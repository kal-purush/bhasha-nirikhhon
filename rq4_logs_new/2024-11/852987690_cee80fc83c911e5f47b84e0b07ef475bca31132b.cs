using Movie.API.Models.Dto;

namespace Movie.API.Services.Handlers.Genres.Queries.GetGenresByMovieId;

public record GetGenresByMovieIdQuery(int MovieId) : IQuery<GetGenresByMovieIdResult>;

public record GetGenresByMovieIdResult(IEnumerable<GenreDto> Genres);


internal class GetGenresByMovieIdQueryHandler(IMovieService movieService)
    : IQueryHandler<GetGenresByMovieIdQuery, GetGenresByMovieIdResult>
{
    public async Task<GetGenresByMovieIdResult> Handle(GetGenresByMovieIdQuery query, 
        CancellationToken cancellationToken)
    {
        var movie = await movieService.GetByIdAsync(query.MovieId, includeGenres: true);

        var genres = movie.MovieGenres.Select(mg => new GenreDto{
            Id = mg.GenreId,
            Name = mg.Genre.Name
        }).ToList();

        return new GetGenresByMovieIdResult(genres);
    }
}