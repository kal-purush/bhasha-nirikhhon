using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using YoTennis.Data;
using YoTennis.Models;

namespace YoTennis.Services
{
    public class DynamicStatsService : IStatsService
    {
        private IMatchListService _matchListService;

        public DynamicStatsService(IMatchListService matchListService)
        {
            _matchListService = matchListService;
        }

        public async Task<IEnumerable<PlayerStatsModel>> GetPlayersStatsModel(string userId)
        {
            var result = new Dictionary<string, PlayerStatsModel>();

            foreach (var matchInfo in await _matchListService.GetMatches(userId, await _matchListService.GetMatchCount(userId), 0))
            {
                var firstPlayerCurrentStats = new PlayerStatsModel
                {
                    Player = matchInfo.FirstPlayer,
                    Matches = 1,
                    Completed = matchInfo.State == MatchState.Completed ? 1 : 0,
                    Won = matchInfo.Winner == Player.First ? 1 : 0,
                    Lost = matchInfo.Winner == Player.Second ? 1 : 0,
                    AggregatedMatchStats =
                    (await (await _matchListService.GetMatchService(userId, matchInfo.MatchId)).GetPlayersMatchStats()).FirstPlayer
                };

                var secondPlayerCurrentStats = new PlayerStatsModel
                {
                    Player = matchInfo.SecondPlayer,
                    Matches = 1,
                    Completed = matchInfo.State == MatchState.Completed ? 1 : 0,
                    Won = matchInfo.Winner == Player.Second ? 1 : 0,
                    Lost = matchInfo.Winner == Player.First ? 1 : 0,
                    AggregatedMatchStats =
                    (await (await _matchListService.GetMatchService(userId, matchInfo.MatchId)).GetPlayersMatchStats()).SecondPlayer
                };
                if (matchInfo.FirstPlayer != null)
                    result[matchInfo.FirstPlayer] = result.TryGetValue(matchInfo.FirstPlayer, out var firstPlayerOldStats)
                        ? firstPlayerOldStats + firstPlayerCurrentStats
                        : firstPlayerCurrentStats;
                if (matchInfo.SecondPlayer != null)
                    result[matchInfo.SecondPlayer] = result.TryGetValue(matchInfo.SecondPlayer, out var secondPlayerOldStats)
                        ? secondPlayerOldStats + secondPlayerCurrentStats
                        : secondPlayerCurrentStats;
            }

            return result.Values.OrderBy(model => model.Player);
        }

        public async Task<PlayerStatsModel> GetPlayerStatsModel(string userId, string player)
        {
            var result = new PlayerStatsModel
            {
                Player = player,
                Matches = 0,
                Completed = 0,
                Won = 0,
                Lost = 0,
                AggregatedMatchStats = new PlayerStatsMatchModel
                {
                    TotalPoints = 0,

                    Ace = 0,
                    Backhand = 0,
                    DoubleFaults = 0,
                    Error = 0,
                    Forehand = 0,
                    NetPoint = 0,
                    UnforcedError = 0,

                    FirstServe = 0,
                    FirstServeSuccessful = 0,
                    WonOnFirstServe = 0,

                    SecondServe = 0,
                    SecondServeSuccessful = 0,
                    WonOnSecondServe = 0
                }
            };

            foreach (var matchInfo in await _matchListService.GetMatches(userId, await _matchListService.GetMatchCount(userId), 0))
            {
                PlayerStatsModel playerCurrentStats = null;

                if (matchInfo.FirstPlayer == player)
                {
                    playerCurrentStats = new PlayerStatsModel
                    {
                        Player = matchInfo.FirstPlayer,
                        Matches = 1,
                        Completed = matchInfo.State == MatchState.Completed ? 1 : 0,
                        Won = matchInfo.Winner == Player.First ? 1 : 0,
                        Lost = matchInfo.Winner == Player.Second ? 1 : 0,
                        AggregatedMatchStats =
                        (await (await _matchListService.GetMatchService(userId, matchInfo.MatchId)).GetPlayersMatchStats()).FirstPlayer
                    };
                }

                if (matchInfo.SecondPlayer == player)
                {
                    playerCurrentStats = new PlayerStatsModel
                    {
                        Player = matchInfo.SecondPlayer,
                        Matches = 1,
                        Completed = matchInfo.State == MatchState.Completed ? 1 : 0,
                        Won = matchInfo.Winner == Player.Second ? 1 : 0,
                        Lost = matchInfo.Winner == Player.First ? 1 : 0,
                        AggregatedMatchStats =
                    (await (await _matchListService.GetMatchService(userId, matchInfo.MatchId)).GetPlayersMatchStats()).SecondPlayer
                    };
                }

                result = playerCurrentStats != null ? result + playerCurrentStats : result;

            }

            return result;
        }
    }
}