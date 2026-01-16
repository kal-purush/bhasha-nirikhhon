using Backend.DataAccessObjects.Favourite;
using Microsoft.AspNetCore.Mvc;
using Shared;

namespace Backend.Controllers;

[ApiController]
[Route("[controller]")]
public class FavouriteController : ControllerBase
{
    private IFavouriteInterface _favouriteInterface;

    public FavouriteController(IFavouriteInterface favouriteInterface)
    {
        _favouriteInterface = favouriteInterface;
    }

    [HttpPost]
    public async Task<ActionResult> AddMovieToFavourite(Favourite favourite)
    {
        try
        {
            await _favouriteInterface.AddFavouriteMovieAsync(favourite);
            return StatusCode(200);
        }
        catch (Exception e)
        {
            return   StatusCode(500, e.Message);
        }
    }
}