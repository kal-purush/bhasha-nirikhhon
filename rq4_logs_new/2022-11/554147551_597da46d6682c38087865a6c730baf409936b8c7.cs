using BLL.Handlers.GameHistories;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using WEB.ViewModels;

namespace WEB.Controllers
{
    [AllowAnonymous]
    public class GameHistoryController : BaseController
    {
        [HttpGet]
        public async Task<ActionResult<IEnumerable<GameHistoryViewModel>>> GetGameHistories()
        {
            var gameHistories = await Mediator.Send(new ListGameHistories.Query());

            var gameHistoryViewModels = new List<GameHistoryViewModel>();

            foreach (var gameHistory in gameHistories)
            {
                gameHistoryViewModels.Add(new GameHistoryViewModel
                {
                    Id = gameHistory.Id,
                    GameId = gameHistory.GameId,
                    FirstPlayerName = gameHistory.FirstPlayerName,
                    SecondPlayerName = gameHistory.SecondPlayerName,
                    GameStateName = gameHistory.GameStateName,
                    WinnerName = gameHistory.WinnerName
                });
            }

            return Ok(gameHistoryViewModels);
        }
    }
}