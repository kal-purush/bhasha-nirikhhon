using Backend.DataAccessObjects.Movies;
using Microsoft.AspNetCore.Mvc;
using Shared;

namespace Backend.Controllers;

[ApiController]
[Route("[controller]")]

public class MovieController : ControllerBase
{
    private IMoviesInterface _moviesInterface;


    public MovieController(IMoviesInterface moviesInterface)
    {
        _moviesInterface = moviesInterface;
    }

    [HttpGet]
    
    public async Task<List<Movie>?> GetAllMoviesAsync()
    {
        return await _moviesInterface.GetAllMoviesAsync();
    }
}