using Domain.Interfaces.ApplicationInterfaces.YoutubeDataService.TeamIdentifiers.IYoutubeTitleTeamOccurenceServices;
using LolMatchFilterNew.Domain.DTOs.YoutubeTitleTeamOccurrenceDTOs;





namespace Application.MatchPairingService.YoutubeDataService.TeamIdentifiers.YoutubeTitleTeamOccurenceServices
{
    public class YoutubeTitleTeamOccurenceService : IYoutubeTitleTeamOccurenceService
    {




        public void UpdateYoutubeTitle(YoutubeTitleTeamOccurenceDTO matchResultDTO, string title)
        {
            matchResultDTO.YoutubeTitle = title;
        }



        public void UpdateMatchingTeamNameIds(YoutubeTitleTeamOccurenceDTO matchResultDTO, string teamNameId, List<string> matches)
        {
            if (matchResultDTO.MatchingTeamNameIds.Keys.Contains(teamNameId))
            {
                return;
            }
            matchResultDTO.MatchingTeamNameIds.Add(teamNameId, matches);
        }





        public void IncrementCount(YoutubeTitleTeamOccurenceDTO matchResultDTO, string countType, int increment)
        {
            switch (countType)
            {
                case "Short":
                    matchResultDTO.ShortNameCount += increment;
                    break;
                case "Medium":
                    matchResultDTO.MediumNameCount += increment;
                    break;
                case "Long":
                    matchResultDTO.LongNameCount += increment;
                    break;
                case "Inputs":
                    matchResultDTO.MatchingInputsCount += increment;
                    break;
                default:
                    throw new ArgumentException($"Invalid count type: {countType}");
            }
        }


        public Dictionary<string, List<string>> GetTopTwoTeamMatches(YoutubeTitleTeamOccurenceDTO matchDTO)
        {
            // Dict to hold counts of all potential matches. 
            Dictionary<string, int> teamIdsWithCountsOfMatches = new Dictionary<string, int>();

            foreach (var teamidMatch in matchDTO.MatchingTeamNameIds)
            {
                string teamId = teamidMatch.Key;
                int idCount = teamidMatch.Value.Count;
                teamIdsWithCountsOfMatches.Add(teamId, idCount);
            }

            // Retrieve top two counts.
            List<KeyValuePair<string, int>> topTwoCounts = teamIdsWithCountsOfMatches
                .OrderByDescending(x => x.Value)
                .Take(2)
                .ToList();

            // Create result dictionary to store matches with top counts
            Dictionary<string, List<string>> result = new Dictionary<string, List<string>>();

            // Get the actual count values from top two entries
            int firstCount = topTwoCounts[0].Value;
            int secondCount = topTwoCounts.Count > 1 ? topTwoCounts[1].Value : firstCount;

            // Add all teams that match either of the top two counts
            foreach (var teamMatch in matchDTO.MatchingTeamNameIds)
            {
                int currentCount = teamMatch.Value.Count;
                if (currentCount == firstCount || currentCount == secondCount)
                {
                    result.Add(teamMatch.Key, teamMatch.Value);
                }
            }

            return result;
        }





    }
}