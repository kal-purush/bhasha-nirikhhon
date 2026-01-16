using car_racing_tournament_api.DTO;
using car_racing_tournament_api.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace car_racing_tournament_api.Controllers
{
    [Route("api/favorite")]
    [ApiController]
    public class FavoriteController : Controller
    {
        private IFavorite _favoriteService;
        private ISeason _seasonService;
        private IUser _userService;
        private IConfiguration _configuration;

        public FavoriteController(IFavorite favoriteService, ISeason seasonService, IUser userService)
        {
            _favoriteService = favoriteService;
            _seasonService = seasonService;
            _userService = userService;
            _configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();;
        }

        [HttpPost, Authorize]
        public async Task<IActionResult> Post([FromBody] FavoriteDto favoriteDto)
        {
            var resultGetSeason = await _seasonService.GetSeasonById(favoriteDto.SeasonId);
            if (!resultGetSeason.IsSuccess)
                return NotFound(resultGetSeason.ErrorMessage);

            var resultGetUser = await _userService.GetUserById(favoriteDto.UserId);
            if (!resultGetUser.IsSuccess)
                return NotFound(resultGetUser.ErrorMessage);

            if (new Guid(User.Identity!.Name!) != resultGetUser.User!.Id)
                return Forbid();

            var resultAdd = await _favoriteService.AddFavorite(resultGetUser.User!, resultGetSeason.Season!);
            if (!resultAdd.IsSuccess)
                return BadRequest(resultAdd.ErrorMessage);

            return NoContent();
        }

        [HttpDelete("{id}"), Authorize]
        public async Task<IActionResult> Delete(Guid id)
        {
            var resultGet = await _favoriteService.GetFavoriteById(id);
            if (!resultGet.IsSuccess)
                return NotFound(resultGet.ErrorMessage);

            if (new Guid(User.Identity!.Name!) != resultGet.Favorite!.UserId)
                return Forbid();

            var resultDelete = await _favoriteService.RemoveFavorite(resultGet.Favorite);
            if (!resultDelete.IsSuccess)
                return BadRequest(resultDelete.ErrorMessage);

            return NoContent();
        }
    }
}