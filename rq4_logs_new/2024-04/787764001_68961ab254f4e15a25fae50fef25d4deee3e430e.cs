using TechTitans.Models;
using TechTitans.Repositories;

namespace TechTitans.Services
{
    public class ArtistSongDashboardController
    {
        private Repository<SongBasicDetails> SongRepo = new Repository<SongBasicDetails>();
        private Repository<SongFeatures> FeatureRepo = new Repository<SongFeatures>();
        private Repository<SongRecommendationDetails> SongRecommendationRepo = new Repository<SongRecommendationDetails>();
        private Repository<AuthorDetails> ArtistRepo = new Repository<AuthorDetails>();


        //converts song details to song info
        public SongBasicInfo songBasicDetailsToInfo(SongBasicDetails song)
        {
            SongBasicInfo songInfo = new SongBasicInfo();
            songInfo.SongId = song.SongId;
            songInfo.Name = song.Name;
            songInfo.Genre = song.Genre;
            songInfo.Subgenre = song.Subgenre;
            songInfo.Language = song.Language;
            songInfo.Country = song.Country;
            songInfo.Album = song.Album;
            songInfo.Image = song.Image;
            foreach (AuthorDetails artist in ArtistRepo.GetAll())
            {
                if (artist.ArtistId == song.ArtistId)
                {
                    songInfo.Artist = artist.Name;
                }
            }
            foreach (SongFeatures feature in FeatureRepo.GetAll())
            {
                if (feature.Song_Id == song.SongId)
                {
                    songInfo.Features.Add(feature.ToString());
                }
            }
            return songInfo;
        }


        //get all artist's publish song (returns List<Entity>)
        public List<SongBasicInfo> getAllArtistSongs(int artistId)
        {
            List<SongBasicInfo> artistSongs = new List<SongBasicInfo>();
            foreach (SongBasicDetails song in SongRepo.GetAll())
            {
                if (song.ArtistId == artistId)
                {
                    SongBasicInfo songInfo = songBasicDetailsToInfo(song);
                    artistSongs.Add(songInfo);
                }
            }
            return artistSongs;
        }

        //search by string in titles (returns list of songs, case insensitive)

        public List<SongBasicInfo> searchByTitle(string title)
        {
            List<SongBasicInfo> songs = new List<SongBasicInfo>();
            foreach (SongBasicDetails song in SongRepo.GetAll())
            {
                if (song.Name.ToLower().Trim().Contains(title.ToLower()))
                {
                    SongBasicInfo songInfo = songBasicDetailsToInfo(song);
                    songs.Add(songInfo);
                }
            }
            return songs;
        }


        //gets song info by song id
        public SongBasicInfo getSongInfo(int songId)
        {
            foreach (SongBasicDetails song in SongRepo.GetAll())
            {
                if (song.SongId == songId)
                {
                    SongBasicInfo songInfo = songBasicDetailsToInfo(song);
                    return songInfo;
                }
            }
            return null;
        }

        //gets song recommendation details by song id
        public SongRecommendationDetails getSongDetails(int songId)
        {
            foreach (SongRecommendationDetails songDetails in SongRecommendationRepo.GetAll())
            {
                if (songDetails.SongId == songId)
                {
                    return songDetails;
                }
            }
            return null;
        }

        //gets artist info by song id
        public AuthorDetails getArtistInfo(int SongId)
        {
            foreach (SongBasicDetails song in SongRepo.GetAll())
            {
                if (song.SongId == SongId)
                {
                    foreach (AuthorDetails artist in ArtistRepo.GetAll())
                    {
                        if (artist.ArtistId == song.ArtistId)
                        {
                            return artist;
                        }
                    }
                }
            }
            return null;
        }


    }


}