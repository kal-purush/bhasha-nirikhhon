using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using LolMatchFilterNew.Domain.Interfaces.InfrastructureInterfaces.IImport_YoutubeDataRepositories;
using Domain.Interfaces.ApplicationInterfaces.IProcessed_YoutubeDataDTOBuilders;
using Domain.DTOs.Processed_YoutubeDataDTOs;
using LolMatchFilterNew.Domain.Entities.Imported_Entities.Import_YoutubeDataEntities;
using LolMatchFilterNew.Domain.Interfaces.ApplicationInterfaces.IYoutubeTeamExtractors;

namespace Application.MatchPairingService.YoutubeDataService.YoutubeTeamNameServices
{
    public class YoutubeTeamNameService
    {
        private readonly IAppLogger _appLogger;
        private readonly IImport_YoutubeDataRepository _import_YoutubeDataRepository;
        private readonly IStoredSqlFunctionCaller _storedSqlFunctionCaller;
        private readonly IProcessed_YoutubeDataDTOBuilder _processed_YoutubeDataDTOBuilder;
        private readonly IYoutubeTeamExtractor _youtubeTeamExtractor;
        


        public YoutubeTeamNameService(IAppLogger appLogger, IImport_YoutubeDataRepository import_YoutubeDataRepository, IStoredSqlFunctionCaller storedSqlFunctionCaller, IProcessed_YoutubeDataDTOBuilder processed_YoutubeDataDTOBuilder, IYoutubeTeamExtractor youtubeTeamExtractor)
        {
            _appLogger = appLogger;
            _import_YoutubeDataRepository = import_YoutubeDataRepository;
            _storedSqlFunctionCaller = storedSqlFunctionCaller;
            _processed_YoutubeDataDTOBuilder = processed_YoutubeDataDTOBuilder;
            _youtubeTeamExtractor = youtubeTeamExtractor;
        }

        public Processed_YoutubeDataDTO ExtractAndBuildProcessedDTO(Import_YoutubeDataEntity youtubeData)
        {

            List<string?> extractedTeams = _youtubeTeamExtractor.ExtractTeamNamesAroundVsKeyword(youtubeData.VideoTitle);

            string? team1 = extractedTeams.Count > 0 ? extractedTeams[0] : null;
            string? team2 = extractedTeams.Count > 1 ? extractedTeams[1] : null;


            return _processed_YoutubeDataDTOBuilder.BuildProcessedDTO(youtubeData, team1, team2);
        }

        public List<Processed_YoutubeDataDTO> BuildProcessed_YoutubeDataDTOList(List<Import_YoutubeDataEntity> youtubeDataEntities)
        {
            List<Processed_YoutubeDataDTO> processed = new List<Processed_YoutubeDataDTO>();

            int teamNullCount = 0;

            List<Processed_YoutubeDataDTO> YoutubeVideosWithNoMatch = new List<Processed_YoutubeDataDTO>();

            foreach (var video in youtubeDataEntities)
            {

                Processed_YoutubeDataDTO newDto = ExtractAndBuildProcessedDTO(video);

                processed.Add(newDto);

                if (newDto.Team1 == null || newDto.Team1 == string.Empty)
                {
                    teamNullCount++;
                }
                if (newDto.Team2 == null || newDto.Team2 == string.Empty)
                {
                    teamNullCount++;
                }
                if (newDto.Team1 == null || newDto.Team1 == string.Empty && newDto.Team2 == null || newDto.Team2 == string.Empty)
                {
                    YoutubeVideosWithNoMatch.Add(newDto);
                }

            }

            if (YoutubeVideosWithNoMatch.Count > 0)
            {
                foreach (var video in YoutubeVideosWithNoMatch)
                {
                    Console.WriteLine($"Playlist title {video.PlaylistTitle}, title: {video.VideoTitle}.");
                }
            }

            _appLogger.Info($"NUMBER OF TEAM EXTRACTIONS FAILED: {teamNullCount}");
            return processed;

        }


    }
}