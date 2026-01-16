using Domain.DTOs.PlayListDateRangeDTOs;
using Domain.DTOs.Western_MatchDTOs;
using Domain.Interfaces.ApplicationInterfaces.IDTOBuilders.PlayListDateRangeServices;
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Entities.Imported_Entities.Import_YoutubeDataEntities;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using LolMatchFilterNew.Domain.Interfaces.InfrastructureInterfaces.IImport_ScoreboardGamesRepositories;
using Xceed.Document.NET;

namespace Application.MatchPairingService.YoutubeDataService.DateIdentifiers.PlayListDateRangeServices
{
    public class PlayListDateRangeService : IPlayListDateRangeService
    {

        private readonly IAppLogger _appLogger;
        private readonly IImport_ScoreboardGamesRepository _import_ScoreboardGamesRepository;
        private readonly IObjectLogger _objectLogger;
        private readonly IStoredSqlFunctionCaller _storedSqlFunctionCaller;

        private List<PlayListDateRangeResult> _gamesWithinYoutubePlaylistDates { get; set; }


        public PlayListDateRangeService(IAppLogger appLogger, IImport_ScoreboardGamesRepository import_ScoreboardGamesRepository, IObjectLogger objectLogger, IStoredSqlFunctionCaller storedSqlFunctionCaller)
        {
            _appLogger = appLogger;
            _import_ScoreboardGamesRepository = import_ScoreboardGamesRepository;
            _objectLogger = objectLogger;
            _storedSqlFunctionCaller = storedSqlFunctionCaller;
            _gamesWithinYoutubePlaylistDates = new List<PlayListDateRangeResult>();
        }


        // Calls GroupVideosByPlaylist to populate a dict of  Import_YoutubeDataEntity with the playlistName as the key, CreatePlayListDateRangeDTO used to create a PlayListDateRangeResult with all of the videos within that range in VideosWithinRange.
        public void PopulateGamesWithinPlaylistDates(List<Import_YoutubeDataEntity> youtubeEntities)
        {
            var playlistDTOs = new List<PlayListDateRangeResult>();

            var groupedVideos = GroupVideosByPlaylist(youtubeEntities);

            foreach (var videoGroup in groupedVideos.Values)
            {
                var dto = CreatePlayListDateRangeDTO(videoGroup);
                playlistDTOs.Add(dto);
            }
            _appLogger.Info("{MethodName} complete with count: {Count} PlaylistDateRangeResults",
                nameof(PopulateGamesWithinPlaylistDates),
                playlistDTOs.Count);

            _gamesWithinYoutubePlaylistDates = playlistDTOs;
        }


        public async Task UpdateGamesWithinYoutubePlaylistDatesWithMatchesAsync()
        {
            if (!_gamesWithinYoutubePlaylistDates.Any())
            {
                throw new InvalidOperationException("The '_gamesWithinYoutubePlaylistDates' collection is empty. Ensure 'PopulateGamesWithinPlaylistDates' is called before 'UpdateGamesWithinYoutubePlaylistDatesWithMatchesAsync'.");
            }

            foreach (var dateRange in _gamesWithinYoutubePlaylistDates)
            {
                await TESTUpdateSinglePlaylistRangeNEW(dateRange);
            }
            _objectLogger.LogPlaylistDateRanges(_gamesWithinYoutubePlaylistDates);
        }










        private Dictionary<string, List<Import_YoutubeDataEntity>> GroupVideosByPlaylist(List<Import_YoutubeDataEntity> youtubeEntities)
        {
            if (youtubeEntities == null)
            {
                throw new ArgumentNullException(nameof(youtubeEntities));
            }

            if (youtubeEntities.Any(x => x.PlaylistTitle == null))
            {
                throw new ArgumentException("One or more videos have null PlaylistTitle", nameof(youtubeEntities));
            }

            return youtubeEntities
                .GroupBy(x => x.PlaylistTitle)
                .ToDictionary(
                    group => group.Key,
                    group => group.ToList()
                );
        }




        private PlayListDateRangeResult CreatePlayListDateRangeDTO(List<Import_YoutubeDataEntity> youtubeEntitiesForOnePlaylist)
        {
            var distinctPlaylistTitles = youtubeEntitiesForOnePlaylist
                .Select(x => x.PlaylistTitle)
                .Distinct()
                .ToList();

            if (distinctPlaylistTitles.Count > 1)
            {
                throw new ArgumentException($"Expected videos from a single playlist but found {distinctPlaylistTitles.Count} different playlists: {string.Join(", ", distinctPlaylistTitles)}");
            }

            return new PlayListDateRangeResult(_appLogger)
            {
                PlaylistName = distinctPlaylistTitles.First(),
                StartDate = youtubeEntitiesForOnePlaylist.Min(v => v.PublishedAt_utc),
                EndDate = youtubeEntitiesForOnePlaylist.Max(v => v.PublishedAt_utc),
                VideosWithinRange = youtubeEntitiesForOnePlaylist
            };
        }







        private async Task TESTUpdateSinglePlaylistRangeNEW(PlayListDateRangeResult dateRange)
        {
            List<WesternMatchDTO> matches = await _storedSqlFunctionCaller.GetWesternMatchesAsync();

            int DateRangeLeeway = 14;

            DateTime rangeStartDate = dateRange.StartDate.AddDays(-DateRangeLeeway);
            DateTime rangeEndDate = dateRange.EndDate.AddDays(DateRangeLeeway);


            var matchesWithinRange = matches.Where(match =>
                match.DateTime_Utc.HasValue &&
                match.DateTime_Utc.Value >= rangeStartDate &&
                match.DateTime_Utc.Value <= rangeEndDate)
                .OrderBy(match => match.DateTime_Utc)
                .ToList();

            _appLogger.Info($"Found {matchesWithinRange.Count} matches");


            dateRange.AddGamesWithinDateRange(matchesWithinRange);


        }







        private async Task UpdateSinglePlaylistRange(PlayListDateRangeResult dateRange)
        {
            List<WesternMatchDTO> matches = await _import_ScoreboardGamesRepository.GetEuNaMatchesWithinDateRangeAsync(dateRange.StartDate, dateRange.EndDate);
            _appLogger.Info($"Searching for games within date range {dateRange.StartDate} - {dateRange.EndDate}.");

            dateRange.AddGamesWithinDateRange(matches);
        }






        public List<PlayListDateRangeResult> ReturngamesWithinYoutubePlaylistDates()
        {
            return _gamesWithinYoutubePlaylistDates;
        }

















    }





}
