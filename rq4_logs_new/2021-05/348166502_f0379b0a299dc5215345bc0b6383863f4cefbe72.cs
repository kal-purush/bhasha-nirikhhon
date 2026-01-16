using Shufl.API.DownloadModels.Music;
using Shufl.API.DownloadModels.User;
using System;
using System.Collections.Generic;

namespace Shufl.API.DownloadModels.Group
{
    public class GroupPlaylistDownloadModel
    {
        public string Id { get; set; }

        public string GroupId { get; set; }

        public string Identifier { get; set; }

        public string PlaylistId { get; set; }

        public PlaylistDownloadModel Playlist { get; set; }

        public IEnumerable<GroupPlaylistRatingDownloadModel> GroupPlaylistRatings { get; set; }

        public DateTime CreatedOn { get; set; }

        public UserDownloadModel CreatedBy { get; set; }
    }
}