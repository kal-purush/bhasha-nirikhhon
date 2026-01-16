using FrontendBlazorWebAssembly.Model;
using FrontendBlazorWebAssembly.Services;
using Microsoft.AspNetCore.Components;
using Shared;

namespace FrontendBlazorWebAssembly.Pages;

public class FavouriteBase : ComponentBase
{
    
    public List<Movie> displayMovies { get; set; } = new();
    public List<Movie> displayMovies1 { get; set; } = new();
    public List<MovieIMG> MovieDetailsList { get; set; } = new();

    public Favourite Favourite { get; set; } = new();
    [Inject] public IMovieService _MovieService { get; set; }

    public string title { get; set; }


    protected override async Task OnInitializedAsync()
    {
        if (!string.IsNullOrWhiteSpace(title))
        {
            displayMovies = await _MovieService.GetAllMoviesByTitle(title);
        }
        else
        {
            displayMovies1 = await _MovieService.GetAllMovies();

            var tasks = displayMovies1.Select(async movies =>
            {
                //  int MovieId = Convert.ToInt32(movies.Id);
                var details = await _MovieService.GetAllMovieDetailsById(movies.Id);
                MovieDetailsList.AddRange(details);
            });

            await Task.WhenAll(tasks);
        }

        displayMovies = displayMovies1;
    }

    public string getimage(long? id)
    {
        Console.WriteLine(id);
        string movieid = "tt" + id;
        var movieDetails = MovieDetailsList.Find(e => e.id == movieid);
        if (movieDetails != null)
        {
            var url = movieDetails.poster_path;
            return url;
        }

        //  var t = await "https://image.tmdb.org/t/p/w342//v4QfYZMACODlWul9doN9RxE99ag.jpg".ToString();
        return null;
    }

  
    public async Task SearchOnInput(ChangeEventArgs e)
    {
        // Update the title property with the input value
        title = e.Value.ToString();

        // Check if the title is not empty, then get movies by title, otherwise get all movies
        if (!string.IsNullOrWhiteSpace(title))
        {
            displayMovies = await _MovieService.GetAllMoviesByTitle(title);
        }
        else
        {
            // If title is empty, initialize the page with all movies
            displayMovies = await _MovieService.GetAllMovies();
        }
    }
    
}