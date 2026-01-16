using Domain.DTOs.YoutubeTitleTeamNameOccurrenceCountDTOs;
using Domain.DTOs.TeamnameDTOs;
using Domain.Interfaces.ApplicationInterfaces.IYoutubeTitleTeamMatchCountFactories;


namespace Application.MatchPairingService.YoutubeDataService.YoutubeTitleTeamMatchCountFactories
{
    public class YoutubeTitleTeamMatchCountFactory : IYoutubeTitleTeamMatchCountFactory
    {


        public YoutubeTitleTeamNameOccurrenceCountDTO InitializeYoutubeTitleTeamMatchCountWithAllCountsZero(TeamNameDTO teamNameDTO, int longNameMatch = 0, int mediumNameMatch = 0, int shortNameMatch = 0, List<string> matchingInputs = null)
        {
            return new YoutubeTitleTeamNameOccurrenceCountDTO
            {
                TeamNameDto = teamNameDTO,
                LongNameMatch = longNameMatch,
                MediumNameMatch = mediumNameMatch,
                ShortNameMatch = shortNameMatch,
                MatchingInputs = matchingInputs ?? new List<string>()
            };
        }



    }
}
