


namespace Application.MatchPairingService.YoutubeDataService.YoutubeTitleTeamNameMatchResults
{
    public class YoutubeTitleTeamNameMatchResult
    {

        public string YoutubeTitle { get; set; } = string.Empty;
        public Dictionary<string, List<string>> MatchingTeamNameIds { get; set; }

        public int LongNameCount { get; set; } = 0;
        public int MediumNameCount { get; set; } = 0;
        public int ShortNameCount { get; set; } = 0;
        public int MatchingInputsCount { get; set; } = 0;   
        public List<string>? MatchingInputs { get; set; }

        public List<string>? Exclusions { get; set; }   


        public void UpdateYoutubeTitle(string title)
        {
            YoutubeTitle = title;
        }


        public void IncrementCount(string countType, int increment)
        {
            switch (countType)
            {
                case "Short":
                    ShortNameCount += increment;
                    break;
                case "Medium":
                    MediumNameCount += increment;
                    break;
                case "Long":
                    LongNameCount += increment;
                    break;
                case "Inputs":
                    MatchingInputsCount += increment;
                    break;
                default:
                    throw new ArgumentException($"Invalid count type: {countType}");
            }
        }


        public void UpdateMatchingTeamNameIds(string teamNameId, List<string> matches)
        {
            MatchingTeamNameIds.Add(teamNameId, matches);
        }


        public Dictionary<string, List<string>> GetTopTwoTeamMatches()
        {
            return MatchingTeamNameIds
                .OrderByDescending(kvp => kvp.Value.Count)
                .Take(2)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }


    }
}