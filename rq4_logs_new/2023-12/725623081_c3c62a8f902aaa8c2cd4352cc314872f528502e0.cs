using FrontendBlazorWebAssembly.Model;
using FrontendBlazorWebAssembly.Services;
using Microsoft.AspNetCore.Components;
using Shared;

namespace FrontendBlazorWebAssembly.Pages;

public class DetailsBase : ComponentBase
{
    [Parameter] public string Id { get; set; }
    public List<Movie?> displayMovies { get; set; } = new();
    public Movie? SelectedMovie { get; set; } = new Movie(); // Property to store the selected movie

    public MovieDetails? MovieDetails { get; set; } = new MovieDetails(); // Property to store the selected movie
    [Inject] public IMovieService _MovieService { get; set; }
    public bool ShowErrorMessage { get; set; }
    public string ErrorMessage { get; set; }


    protected async override Task OnInitializedAsync()
    {
        // If Id is null, then it gets assigned the value "1".
        Id = Id ?? "1";

        // Get all movies
        displayMovies = await _MovieService.GetAllMovies();

        // Find the selected movie by ID
        SelectedMovie = displayMovies.FirstOrDefault(m => m.Id.ToString() == Id);

        int MovieId = Convert.ToInt32(Id);

        try
        {
            MovieDetails = await _MovieService.GetMovieDetails(MovieId);
        }
        catch (Exception ex)
        {
            // Handle other exceptions
            ShowErrorMessage = true;
            ErrorMessage = "An error occurred while fetching movie details.";
            Console.WriteLine($"Exception: {ex}");
        }

        if (SelectedMovie == null)
        {
            // Handle the case where the movie with the specified ID is not found
            // Set flag and message to display on the current page
            ShowErrorMessage = true;
            ErrorMessage = "Movie not found";
        }

        if (MovieDetails == null)
        {
            // Handle the case where the movie with the specified ID is not found
            // Set flag and message to display on the current page
            ShowErrorMessage = true;
            ErrorMessage = "Movie not found";
        }
    }
}