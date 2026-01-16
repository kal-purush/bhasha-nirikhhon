using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using YoTennis.Helpers;
using YoTennis.Models;

namespace YoTennis.Data
{
    public static class Conversion
    {
        public static MatchInfo ToMatchInfo(this MatchModel model)
        {
            var matchInfo = new MatchInfo();

            //matchInfo.MatchId;

            matchInfo.FirstPlayer = model.FirstPlayer;
            matchInfo.SecondPlayer = model.SecondPlayer;
            matchInfo.State = model.State;
                        
            if (model.State != MatchState.NotStarted)
                matchInfo.MatchStartedAt = model.MatchStartedAt;

            if (model.State != MatchState.NotStarted && model.State != MatchState.Drawing)
                matchInfo.MatchScore = ForScoreExtensions.Separate(model);

            if (model.State == MatchState.Completed)
                matchInfo.Winner = model.MatchScore.FirstPlayer > model.MatchScore.SecondPlayer ? Player.First : Player.Second;

            return matchInfo;
        }
    }
}