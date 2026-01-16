using Mapster;
using Microsoft.AspNetCore.Mvc;
using TmTracker.Services;

namespace TmTracker.Controllers;

[ApiController]
[Route("[controller]")]
public class GameScoresController(TrackedGameService trackedGameService) : ControllerBase
{
    [HttpGet]
    public async Task<IEnumerable<Models.GameScores>> Get()
        => await trackedGameService.GetGameScores();

    [HttpPut]
    public async Task Put([FromBody]Models.GameScores gameScores)
        => await trackedGameService.SetGameScores(gameScores);
}