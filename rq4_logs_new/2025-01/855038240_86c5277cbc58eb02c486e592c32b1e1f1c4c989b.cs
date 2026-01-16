using Domain.Interfaces.ApplicationInterfaces.IMatchDTOServices.IImport_TeamNameServices;
using Domain.Interfaces.ApplicationInterfaces.IYoutubeTitleTeamNameFinders;
using Domain.Interfaces.ApplicationInterfaces.YoutubeDataService.TeamIdentifiers.IYoutubeTitleTeamOccurrenceServices;
using LolMatchFilterNew.Domain.DTOs.YoutubeTitleTeamOccurrenceDTOs;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using System.Text.RegularExpressions;


namespace Application.MatchPairingService.YoutubeDataService.TeamIdentifiers.YoutubeTitleTeamOccurenceServices
{
    public class YoutubeTitleTeamOccurrenceService : IYoutubeTitleTeamOccurenceService
    {
        private static readonly Regex G2_Game2_FalsePositive = new Regex(@"[Gg]2$", RegexOptions.Compiled);
        private readonly IAppLogger _appLogger;
        private readonly IYoutubeTitleTeamNameFinder _youtubeTitleTeamNameFinder;
        private readonly IImport_TeamNameService _importTeamNameService;



        public void TallyTeamNameOccurrences(YoutubeTitleTeamOccurenceDTO occurrenceDTO)
        {
            string normalizedTitle = occurrenceDTO.YoutubeTitle.ToLower();

            // Check if title ends with G2
            bool endsWithG2 = G2_Game2_FalsePositive.IsMatch(normalizedTitle);

            // If it does, create a temporary version without G2 for matching
            string titleForMatching = endsWithG2
                ? normalizedTitle.Substring(0, normalizedTitle.Length - 2).Trim()
                : normalizedTitle;



            foreach (var teamNameDto in _importTeamNameService.ReturnImport_TeamNameAllNames())
            {
                var matchesForTeam = new List<string>();

                if (teamNameDto.ShortName != null && _youtubeTitleTeamNameFinder.CheckNameMatch(teamNameDto.ShortName, normalizedTitle))
                {
                    IncrementCount(occurrenceDTO, "Short", 1);
                }

                if (teamNameDto.MediumName != null && _youtubeTitleTeamNameFinder.CheckNameMatch(teamNameDto.MediumName, normalizedTitle))
                {
                    IncrementCount(occurrenceDTO, "Medium", 1);
                    matchesForTeam.Add(teamNameDto.MediumName);
                }

                if (teamNameDto.LongName != null && _youtubeTitleTeamNameFinder.CheckNameMatch(teamNameDto.LongName, normalizedTitle))
                {
                    IncrementCount(occurrenceDTO, "Long", 1);
                    matchesForTeam.Add(teamNameDto.LongName);
                }


                if (teamNameDto.FormattedInputs != null)
                {
                    foreach (var input in teamNameDto.FormattedInputs)
                    {
                        if (_youtubeTitleTeamNameFinder.CheckNameMatch(input, normalizedTitle))
                        {
                            IncrementCount(occurrenceDTO, "Inputs", 1);
                            matchesForTeam.Add(input);
                        }
                    }
                }

                if (matchesForTeam.Any())
                {
                    UpdateMatchingTeamNameIds(occurrenceDTO, teamNameDto.LongName, matchesForTeam);
                }
            }
        }



        public YoutubeTitleTeamOccurrenceService(IAppLogger appLogger, IYoutubeTitleTeamNameFinder youtubeTitleTeamNameFinder, IImport_TeamNameService importTeamNameService)
        {
            _appLogger = appLogger;
            _youtubeTitleTeamNameFinder = youtubeTitleTeamNameFinder;
            _importTeamNameService = importTeamNameService;
        }



        public void UpdateYoutubeTitle(YoutubeTitleTeamOccurenceDTO matchResultDTO, string title)
        {
            matchResultDTO.YoutubeTitle = title;
        }



        public void UpdateMatchingTeamNameIds(YoutubeTitleTeamOccurenceDTO matchResultDTO, string teamNameId, List<string> matches)
        {
            if (matchResultDTO.AllMatchingTeamNameIds.Keys.Contains(teamNameId))
            {
                return;
            }
            matchResultDTO.AllMatchingTeamNameIds.Add(teamNameId, matches);
        }


        public void IncrementCount(YoutubeTitleTeamOccurenceDTO matchResultDTO, string countType, int increment)
        {
            if (countType != "Short" && countType != "Medium" && countType != "Long" && countType != "Inputs")
            {
                throw new ArgumentException($"Invalid count type: {countType}. Allowed values are: Short, Medium, Long, Inputs.");
            }

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
            }
        }



        public Dictionary<string, List<string>> GetTeamIdsWithHighestOccurences(YoutubeTitleTeamOccurenceDTO matchDTO)
        {
            // Dict to hold counts of all potential matches. 
            Dictionary<string, int> teamIdsWithCountsOfMatches = new Dictionary<string, int>();

            foreach (var teamidMatch in matchDTO.AllMatchingTeamNameIds)
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
            foreach (var teamMatch in matchDTO.AllMatchingTeamNameIds)
            {
                int currentCount = teamMatch.Value.Count;
                if (currentCount == firstCount || currentCount == secondCount)
                {
                    result.Add(teamMatch.Key, teamMatch.Value);
                }
            }

            return result;
        }

        public void PopulateTeamIdsWithMostMatches(YoutubeTitleTeamOccurenceDTO matchDTO, Dictionary<string, List<string>> teamIdWithMatches)
        {
            matchDTO.TeamIdsWithMostMatches = teamIdWithMatches;
        }


        // EXCLUSION LOGIC MUST CHANGE





    }
}