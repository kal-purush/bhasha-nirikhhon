using System;
using movieGame.Models;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using movieGame.Infrastructure;

namespace movieGame.Controllers.PlayTeamGameController
{
    [Route("group")]
    public class PlayTeamGameController : Controller
    {
        private MovieContext _context;
        public Helpers _h = new Helpers();

        public PlayTeamGameController (MovieContext context)
        {
            _context = context;
        }


        #region MANAGE ROUTING ------------------------------------------------------------

            [HttpGet("")]
            public async Task<IActionResult> PlayGroupGame()
            {
                ViewBag.FirstTeamName = GetTeamNameFromSession("FirstTeamName");
                ViewBag.SecondTeamName = GetTeamNameFromSession("SecondTeamName");
                GetTeamInfo(1);
                await CreateMovieTeamJoin(1, 2, true);
                await CreateMovieTeamJoin(1, 3, true);
                return View("playgroup");
            }

            [HttpGet("add_teams")]
            public IActionResult ViewAddTeamPage()
            {
                return View("newteamform");
            }

        #endregion MANAGE ROUTING ------------------------------------------------------------



        #region SET TEAMS ------------------------------------------------------------

            [HttpPost("set_teams")]
            public async Task<IActionResult> SetTeams (string firstTeamName, string secondTeamName)
            {
                SetTeamNamesInSession(firstTeamName, secondTeamName);

                await CreateNewTeam(firstTeamName);
                await CreateNewTeam(secondTeamName);

                return RedirectToAction("PlayGroupGame");
            }

            [HttpPost("set_game")]
            public async Task SetGame (string firstTeamName, string secondTeamName)
            {
                await CreateNewGame(firstTeamName, secondTeamName);
            }

        #endregion SET TEAMS ------------------------------------------------------------




        #region SET/GET SESSION VARIABLES ------------------------------------------------------------

            public void SetTeamNamesInSession(string firstTeamName, string secondTeamName)
            {
                SetTeamNameInSession("FirstTeamName", firstTeamName);
                SetTeamNameInSession("SecondTeamName", secondTeamName);
            }

            public List<string> GetTeamNamesFromSession()
            {
                List<string> teamNames = new List<string>();
                teamNames.Add(GetTeamNameFromSession("FirstTeamName"));
                teamNames.Add(GetTeamNameFromSession("SecondTeamName"));
                return teamNames;
            }

            public void SetTeamNameInSession(string key, string teamName)
            {
                HttpContext.Session.SetString(key, teamName);
            }

            public string GetTeamNameFromSession(string key)
            {
                string teamName = HttpContext.Session.GetString(key);
                return teamName;
            }

            public void SetTeamIdInSessionFromTeamName(string teamName, int teamId)
            {
                HttpContext.Session.SetInt32(teamName, teamId);
            }

            public void SetTeamIdsInSession(int firstTeamId, int secondTeamId)
            {
                HttpContext.Session.SetInt32("FirstTeamId", firstTeamId);
                HttpContext.Session.SetInt32("SecondTeamId", secondTeamId);
            }

            public int GetTeamIdFromSession(string teamName)
            {
                int teamId = (int)HttpContext.Session.GetInt32(teamName);
                return teamId;
            }

            public List<int> GetTeamIdsFromSession()
            {
                List<int> teamIds = new List<int>();
                teamIds.Add(GetTeamIdFromSession("FirstTeamId"));
                teamIds.Add(GetTeamIdFromSession("SecondTeamId"));
                return teamIds;
            }

            public void SetGameIdInSession(int gameId)
            {
                HttpContext.Session.SetInt32("GameId", gameId);
            }

            public int GetGameIdFromSession()
            {
                int gameId = (int)HttpContext.Session.GetInt32("GameId");
                return gameId;
            }

        #endregion SET/GET SESSION VARIABLES ------------------------------------------------------------



        #region CREATE NEW DATABASE RECORD ------------------------------------------------------------

            public async Task<Team> CreateNewTeam (string teamName)
            {
                var newTeam = new Team ()
                {
                    TeamName = teamName,
                    TeamPoints = 0,
                    GamesPlayed = 0,
                    CountOfMoviesGuessedCorrectly = 0,
                    CountOfMoviesGuessedIncorrectly = 0,
                    MovieTeamJoin = new List<MovieTeamJoin> (),
                    GameTeamJoin = new List<GameTeamJoin> (),
                };
                await AddAndSaveChangesAsync(newTeam);

                SetTeamIdInSessionFromTeamName(newTeam.TeamName, newTeam.TeamId);

                return newTeam;
            }

            public async Task<Models.Game> CreateNewGame(string firstTeamName, string secondTeamName)
            {
                Models.Game newGame = new Models.Game
                {
                    NumberOfTeamsInGame = 2,
                    FirstTeam = new Team(),
                    SecondTeam = new Team(),
                    GameTeamJoin = new List<GameTeamJoin>()
                };

                newGame.FirstTeam.TeamName = "TeamC";
                newGame.SecondTeam.TeamName = "TeamD";

                await AddAndSaveChangesAsync(newGame);

                SetGameIdInSession(newGame.GameId);

                return newGame;
            }

            public async Task<GameTeamJoin> CreateGameTeamJoin(int gameId, int teamId, bool winTrueOrFalse)
            {
                GameTeamJoin newGameTeamJoin = new GameTeamJoin
                {
                    GameId = gameId,
                    TeamId = teamId,
                    WinFlag = winTrueOrFalse,
                };

                if(winTrueOrFalse == true)
                {
                    Console.WriteLine("hold");
                }

                await AddAndSaveChangesAsync(newGameTeamJoin);

                return newGameTeamJoin;
            }

            // public async Task<ActionResult<MovieTeamJoin>> CreateMovieTeamJoin(int teamId, int movieId, bool win)
            public async Task<MovieTeamJoin> CreateMovieTeamJoin(int teamId, int movieId, bool win)
            {
                MovieTeamJoin newMovieTeamJoin = new MovieTeamJoin
                {
                    TeamId = teamId,
                    MovieId = movieId,
                    WinFlag = win
                };

                if(win == true)
                    newMovieTeamJoin.PointsReceived = 5;
                    newMovieTeamJoin.ClueGameWonAt = 5;

                if(win == false)
                    newMovieTeamJoin.PointsReceived = 0;
                    newMovieTeamJoin.ClueGameWonAt = 0;

                await AddAndSaveChangesAsync(newMovieTeamJoin);
                return newMovieTeamJoin;
            }

            public async Task AddAndSaveChangesAsync(object obj)
            {
                _context.Add(obj);
                await _context.SaveChangesAsync();
            }

        #endregion CREATE NEW DATABASE RECORD ------------------------------------------------------------



        #region GET INFO FROM DATABASE ------------------------------------------------------------

            public Team GetTeamInfo(int teamId)
            {
                Team thisTeam = _context.Teams
                    .Include(gtj => gtj.GameTeamJoin)
                    .ThenInclude(g => g.Game)
                    .Include(mtj => mtj.MovieTeamJoin)
                    .ThenInclude(m => m.Movie)
                    .SingleOrDefault(team => team.TeamId == teamId);

                _h.Dig(thisTeam);
                return thisTeam;
            }

            public Models.Game GetGameInfo(int gameId)
            {
                Models.Game thisGame = _context.Games
                    .Include(gtj => gtj.GameTeamJoin)
                    .ThenInclude(t => t.Team)
                    .SingleOrDefault(game => game.GameId == gameId);

                _h.Dig(thisGame);
                return thisGame;
            }

            public List<int> GetMoviesGuessedAlready()
            {
                List<int> movieIdsGuessed = new List<int>();

                return movieIdsGuessed;
            }

        #endregion GET INFO FROM DATABASE ------------------------------------------------------------



        #region CONNECT WITH JAVASCRIPT ------------------------------------------------------------

            [HttpGet("get_clue_number_from_javascript")]
            public JsonResult GetClueNumberFromJavaScript(int clueNumber)
            {
                Console.WriteLine($"get_clue_number_from_javascript --> {clueNumber}");
                return Json(clueNumber);
            }

        #endregion CONNECT WITH JAVASCRIPT ------------------------------------------------------------



    }
}