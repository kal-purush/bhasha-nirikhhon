namespace Movie.API.Services.Handlers.Movies.Commands.AddGenresToMovie;

public record AddGenresToMovieCommand(List<int> GenreIds, int MovieId) 
    : ICommand<AddGenresToMovieResult>;

public record AddGenresToMovieResult(bool IsSucces);

internal class AddGenresToMovieCommandHandler(IMovieGenreRepository movieGenreRepository)
    : ICommandHandler<AddGenresToMovieCommand, AddGenresToMovieResult>
{
    public async Task<AddGenresToMovieResult> Handle(AddGenresToMovieCommand command, 
        CancellationToken cancellationToken)
    {
        List<Models.MovieGenre> movieGenres = new();
        try
        {
            foreach (var genreId in command.GenreIds)
            {
                var movieGenre = new Models.MovieGenre
                {
                    GenreId = genreId,
                    MovieId = command.MovieId
                };

                await movieGenreRepository.CreateAsync(movieGenre);
            }
        }
        catch (Exception ex) 
        {
            throw;
        }

        return new AddGenresToMovieResult(true);
    }
}