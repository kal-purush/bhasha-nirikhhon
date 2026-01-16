using Domain.DTOs.PlayListDateRangeDTOs;
using Google.Apis.YouTube.v3.Data;
using LolMatchFilterNew.Domain.Entities.Imported_Entities.Import_YoutubeDataEntities;
using Domain.Interfaces.ApplicationInterfaces.IDTOBuilders.IPlayListDateRangeDTOFactories;

namespace Application.DTOFactories.PlayListDateRangeDTOFactories
{ 
    public class PlayListDateRangeDTOFactory : IPlayListDateRangeDTOFactory
    {

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

        private PlayListDateRangeDTO CreatePlayListDateRangeDTO(List<Import_YoutubeDataEntity> youtubeEntitiesForOnePlaylist)
        {
            var distinctPlaylistTitles = youtubeEntitiesForOnePlaylist
                .Select(x => x.PlaylistTitle)
                .Distinct()
                .ToList();

            if (distinctPlaylistTitles.Count > 1)
            {
                throw new ArgumentException($"Expected videos from a single playlist but found {distinctPlaylistTitles.Count} different playlists: {string.Join(", ", distinctPlaylistTitles)}");
            }

            return new PlayListDateRangeDTO
            {
                PlaylistName = distinctPlaylistTitles.First(),
                StartDate = youtubeEntitiesForOnePlaylist.Min(v => v.PublishedAt_utc) ?? DateTime.MinValue,
                EndDate = youtubeEntitiesForOnePlaylist.Max(v => v.PublishedAt_utc) ?? DateTime.MinValue,
                Videos = youtubeEntitiesForOnePlaylist
            };
        }

        public List<PlayListDateRangeDTO> CreateListOfPlaylistDateRangeDTOs(List<Import_YoutubeDataEntity> youtubeEntities)
        {
            var playlistDTOs = new List<PlayListDateRangeDTO>();

            var groupedVideos = GroupVideosByPlaylist(youtubeEntities);

            foreach (var videoGroup in groupedVideos.Values)
            {
                var dto = CreatePlayListDateRangeDTO(videoGroup);
                playlistDTOs.Add(dto);
            }

            return playlistDTOs;
        }
    }


}
