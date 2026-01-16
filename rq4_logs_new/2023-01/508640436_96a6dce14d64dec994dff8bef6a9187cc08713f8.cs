using car_racing_tournament_api.Interfaces;
using car_racing_tournament_api.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace car_racing_tournament_api.Controllers
{
    [Route("api/user-season")]
    [ApiController]
    public class UserSeasonController : Controller
    {
        private IUserSeason _userSeasonService;

        public UserSeasonController(IUserSeason userSeasonService)
        {
            _userSeasonService = userSeasonService;
        }

        [HttpDelete("{moderatorId}"), Authorize]
        public async Task<IActionResult> Delete(Guid seasonId, Guid moderatorId)
        {
            if (!await _userSeasonService.HasPermissionAsync(new Guid(User.Identity!.Name!), seasonId))
                return StatusCode(StatusCodes.Status403Forbidden);

            var resultDelete = await _userSeasonService.RemovePermission(new Guid(User.Identity!.Name!), moderatorId, seasonId);
            if (!resultDelete.IsSuccess)
                return BadRequest(resultDelete.ErrorMessage);

            return NoContent();
        }
    }
}