using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MLBPickem.Data;
using MLBPickem.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MLBPickem.ViewComponents
{
    public class GetUserScoreViewModel
    {
        public int UserScore { get; set; } = 0;
    }

    // ViewComponent for displaying current user's score
    public class UserScoreViewComponent : ViewComponent
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public UserScoreViewComponent(ApplicationDbContext context,
        UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }

        private Task<ApplicationUser> GetCurrentUserAsync() => _userManager.GetUserAsync(HttpContext.User);

        [Authorize]
        public async Task<IViewComponentResult> InvokeAsync()
        {
            var user = await GetCurrentUserAsync();

            // Get all user games for complete games, grouped by user
            var completedGamesByUser = await _context.UserGames
                .Where(ug => ug.UserId == user.Id)
                .Include(ug => ug.Game)
                .ThenInclude(g => g.AwayTeam)
                .Include(ug => ug.Game)
                .ThenInclude(g => g.HomeTeam)
                .Where(ug => ug.Game.GameComplete == true)
                .ToListAsync();
            ;

            // Set TotalScore initially to zero
            int userScore = 0;

            completedGamesByUser.ForEach(userGame =>
            {
                // IF USER CHOSE WINNING TEAM
                if (userGame.ChosenTeamId == userGame.Game.WinningTeamId)
                {
                    // IF USER CHOSE AWAY TEAM
                    if (userGame.ChosenTeamId == userGame.Game.AwayTeam.TeamId)
                    {
                        //IF USER CHOSE UNDERDOG
                        if (userGame.Game.AwayLine.StartsWith("+"))
                        {
                            userScore = userScore + Int32.Parse(userGame.Game.AwayLine);
                        }
                        //IF USER CHOSE FAVORITE
                        else
                        {
                            userScore = userScore + 100;
                        }
                    }
                    // IF USER CHOSE HOME TEAM
                    else
                    {
                        //IF USER CHOSE UNDERDOG
                        if (userGame.Game.HomeLine.StartsWith("+"))
                        {
                            userScore = userScore + Int32.Parse(userGame.Game.HomeLine);
                        }
                        //IF USER CHOSE FAVORITE
                        else
                        {
                            userScore = userScore + 100;
                        }

                    }
                }
                // IF USER CHOSE LOSING TEAM
                else
                {
                    // IF USER CHOSE AWAY TEAM
                    if (userGame.ChosenTeamId == userGame.Game.AwayTeam.TeamId)
                    {
                        //IF USER CHOSE UNDERDOG
                        if (userGame.Game.AwayLine.StartsWith("+"))
                        {
                            userScore = userScore - 100;
                        }
                        //IF USER CHOSE FAVORITE
                        else
                        {
                            userScore = userScore - Int32.Parse(userGame.Game.AwayLine);
                        }
                    }
                    // IF USER CHOSE HOME TEAM
                    else
                    {
                        //IF USER CHOSE UNDERDOG
                        if (userGame.Game.HomeLine.StartsWith("+"))
                        {
                            userScore = userScore - 100;
                        }
                        //IF USER CHOSE FAVORITE
                        else
                        {
                            userScore = userScore - Int32.Parse(userGame.Game.HomeLine);
                        }

                    }
                }
            });

            GetUserScoreViewModel model = new GetUserScoreViewModel()
            {
                UserScore = userScore
            };

            return View(model);
        }













    }
}