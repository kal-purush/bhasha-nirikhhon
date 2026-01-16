using Domain.DTOs.PlayListDateRangeDTOs;
using Domain.DTOs.YoutubeVideoWithMatchedTeamDTOs;
using Domain.Interfaces.ApplicationInterfaces.IDTOBuilders.PlayListDateRangeServices;
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using Application.MatchPairingService.YoutubeDataService.YoutubeTitleTeamNameMatchResults;

namespace Application.MatchPairingService.MatchComparisonResultService.YoutubeVideoWithMatchedTeamServices
{
    public class YoutubeVideoWithMatchedTeamService
    {
        private readonly IAppLogger _appLogger;
        private readonly IObjectLogger _objectLogger;
        private readonly IPlayListDateRangeService _playListDateRangeService;



        public YoutubeVideoWithMatchedTeamService(IAppLogger appLogger, IObjectLogger objectLogger, IPlayListDateRangeService playListDateRangeService)
        {
            _appLogger = appLogger;
            _objectLogger = objectLogger;
            _playListDateRangeService = playListDateRangeService;
        }


        public YoutubeVideoWithMatchedTeamDTO CreateYoutubeVideoWithMatchedTeamDTO(string youtubeVideoId, string team1, string team2)
        {
            if (string.IsNullOrWhiteSpace(youtubeVideoId))
            {
                throw new ArgumentException($"YouTube video ID cannot be null or empty", nameof(youtubeVideoId));
            }

            if (string.IsNullOrWhiteSpace(team1))
            {
                throw new ArgumentException($"Team 1 cannot be null or empty", nameof(team1));
            }

            if (string.IsNullOrWhiteSpace(team2))
            {
                throw new ArgumentException($"Team 2 cannot be null or empty", nameof(team2));
            }

            return new YoutubeVideoWithMatchedTeamDTO
            {
                YoutubeVideoId = youtubeVideoId,
                Team1 = team1,
                Team2 = team2
            };
        }


        public List<YoutubeVideoWithMatchedTeamDTO> CreateListOfYoutubeVideoWithMatchedTeamDTOs(List<YoutubeTitleTeamNameMatchResult> youtubeTitleTeamNameMatches)
        {
            throw new NotImplementedException();

        }




    }
}