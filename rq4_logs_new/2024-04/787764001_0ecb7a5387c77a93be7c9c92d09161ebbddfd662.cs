using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Repositories;
using TechTitans.Models;

namespace TechTitans.Repositories
{
    public interface ISongBasicDetailsRepository : IRepository<SongDataBaseModel>
    {
        SongBasicInformation TransformSongBasicDetailsToSongBasicInfo(SongDataBaseModel song);

        SongDataBaseModel GetSongBasicDetails(int songId);

        List<SongDataBaseModel> GetTop5MostListenedSongs(int userId);

        Tuple<SongDataBaseModel, decimal> GetMostPlayedSongPercentile(int userId);

        Tuple<string, decimal> GetMostPlayedArtistPercentile(int userId);

        List<string> GetTop5Genres(int userId);

        List<string> GetAllNewGenresDiscovered(int userId);

    }
}