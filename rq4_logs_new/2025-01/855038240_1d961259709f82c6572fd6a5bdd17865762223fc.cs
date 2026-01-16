using Domain.DTOs.Western_MatchDTOs;
using Domain.Interfaces.ApplicationInterfaces.IDTOBuilders.IWesternMatchDTOFactories;

namespace Application.DTOFactories.WesternMatchDTOFactories
{
    public class WesternMatchDTOFactory : IWesternMatchDTOFactory
    {
        public WesternMatchDTO CreateWesternMatchDTO(
            string gameId,
            string matchId,
            DateTime? dateTimeUtc,
            string? tournament,
            string? team1,
            string? team1TeamId,
            string? team1Players,
            string? team1Picks,
            string? team2,
            string? team2TeamId,
            string? team2Players,
            string? team2Picks,
            string? winTeam,
            string? lossTeam,
            string? team1Region,
            string? team1Longname,
            string? team1Medium,
            string? team1Short,
            List<string>? team1Inputs,
            string? team2Region,
            string? team2Longname,
            string? team2Medium,
            string? team2Short,
            List<string>? team2Inputs)
        {
            return new WesternMatchDTO
            {
                Game_Id = gameId,
                Match_Id = matchId,
                DateTime_Utc = dateTimeUtc,
                Tournament = tournament,
                Team1 = team1,
                Team1Team_Id = team1TeamId,
                Team1_Players = team1Players,
                Team1_Picks = team1Picks,
                Team2 = team2,
                Team2Team_Id = team2TeamId,
                Team2_Players = team2Players,
                Team2_Picks = team2Picks,
                Win_Team = winTeam,
                Loss_Team = lossTeam,
                Team1_Region = team1Region,
                Team1_Longname = team1Longname,
                Team1_Medium = team1Medium,
                Team1_Short = team1Short,
                Team1_Inputs = team1Inputs,
                Team2_Region = team2Region,
                Team2_Longname = team2Longname,
                Team2_Medium = team2Medium,
                Team2_Short = team2Short,
                Team2_Inputs = team2Inputs
            };
        }
    }
}