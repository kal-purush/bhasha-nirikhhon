using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Moq;
using Xunit;
using TechTitans.Repositories;
using TechTitans.Models;

namespace TechTitansTesting.Repositories
{
    public class SongBasicDetailsRepositoryTests
    {
        private Mock<IDatabaseOperations> _mockDatabaseOperations;
        private SongBasicDetailsRepository _repository;

        public SongBasicDetailsRepositoryTests()
        {
            _mockDatabaseOperations = new Mock<IDatabaseOperations>();
            _repository = new SongBasicDetailsRepository(_mockDatabaseOperations.Object);
        }

        [Fact]
        public void TransformSongBasicDetailsToSongBasicInfo_ShouldReturnSongBasicInfo()
        {
            var songBasicDetails = new SongDataBaseModel
            {
                Artist_Id = 1,
                Song_Id = 1,
                Name = "Test Song",
                Genre = "TestGenre",
                Subgenre = "TestSubgenre",
                Language = "TestLanguage",
                Country = "TestCountry",
                Album = "TestAlbum",
                Image = "TestImage.png"
            };
            _mockDatabaseOperations.Setup(c => c.Query<string>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(new List<string> { "Test Artist" });

            var result = _repository.TransformSongBasicDetailsToSongBasicInfo(songBasicDetails);

            Assert.NotNull(result);
            Assert.Equal(1, result.SongId);
            Assert.Equal("Test Song", result.Name);
            Assert.Equal("TestGenre", result.Genre);
            Assert.Equal("TestSubgenre", result.Subgenre);
            Assert.Equal("Test Artist", result.Artist);
            Assert.Equal("TestLanguage", result.Language);
            Assert.Equal("TestCountry", result.Country);
            Assert.Equal("TestAlbum", result.Album);
            Assert.Equal("TestImage.png", result.Image);
        }


        [Fact]
        public void GetSongBasicDetails_WhenSongExists_ShouldReturnSongDetails()
        {
            var songId = 1;
            var expectedSong = new SongDataBaseModel
            {
                Song_Id = songId,
                Name = "Test Song",
                Genre = "TestGenre",
                Subgenre = "TestSubgenre",
                Language = "TestLanguage",
                Country = "TestCountry",
                Album = "TestAlbum",
                Image = "TestImage.png"
            };
            _mockDatabaseOperations.Setup(c => c.Query<SongDataBaseModel>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(new List<SongDataBaseModel> { expectedSong });

            var result = _repository.GetSongBasicDetails(songId);

            Assert.NotNull(result);
            Assert.Equal(songId, result.Song_Id);
            Assert.Equal("Test Song", result.Name);
            Assert.Equal("TestGenre", result.Genre);
            Assert.Equal("TestSubgenre", result.Subgenre);
            Assert.Equal("TestLanguage", result.Language);
            Assert.Equal("TestCountry", result.Country);
            Assert.Equal("TestAlbum", result.Album);
            Assert.Equal("TestImage.png", result.Image);
        }


        [Fact]
        public void GetTop5MostListenedSongs_WhenSongsExist_ShouldReturnTop5Songs()
        {
            var userId = 1;
            var expectedSongs = new List<SongDataBaseModel>
            {
                new SongDataBaseModel { Song_Id = 1, Name = "Song 1", Genre = "Genre1", Subgenre = "Subgenre1", Language = "Language1", Country = "Country1", Album = "Album1", Image = "Image1.png" },
                new SongDataBaseModel { Song_Id = 2, Name = "Song 2", Genre = "Genre2", Subgenre = "Subgenre2", Language = "Language2", Country = "Country2", Album = "Album2", Image = "Image2.png" },
                new SongDataBaseModel { Song_Id = 3, Name = "Song 3", Genre = "Genre3", Subgenre = "Subgenre3", Language = "Language3", Country = "Country3", Album = "Album3", Image = "Image3.png" },
                new SongDataBaseModel { Song_Id = 4, Name = "Song 4", Genre = "Genre4", Subgenre = "Subgenre4", Language = "Language4", Country = "Country4", Album = "Album4", Image = "Image4.png" },
                new SongDataBaseModel { Song_Id = 5, Name = "Song 5", Genre = "Genre5", Subgenre = "Subgenre5", Language = "Language5", Country = "Country5", Album = "Album5", Image = "Image5.png" }
            };
            _mockDatabaseOperations.Setup(c => c.Query<SongDataBaseModel>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(expectedSongs);

            var result = _repository.GetTop5MostListenedSongs(userId);

            Assert.Equal(5, result.Count);
            Assert.Equal("Song 1", result[0].Name);
            Assert.Equal("Song 5", result[4].Name);
        }

        [Fact]
        public void GetMostPlayedSongPercentile_ShouldReturnCorrectPercentile()
        {
            var userId = 1;
            var mostPlayedSong = new SongDataBaseModel { Song_Id = 1, Name = "Most Played Song", Genre = "Genre1", Subgenre = "Subgenre1", Language = "Language1", Country = "Country1", Album = "Album1", Image = "Image1.png" };
            var totalSongs = 10;
            var mostListenedSongCount = 5;

            _mockDatabaseOperations.Setup(c => c.Query<SongDataBaseModel>(It.IsAny<string>(), It.Is<object>(o => o.GetType().GetProperty("userId").GetValue(o, null).Equals(userId)), null, true, null, null))
                                   .Returns(new List<SongDataBaseModel> { mostPlayedSong });

            _mockDatabaseOperations.Setup(c => c.Query<int>(It.IsAny<string>(), It.Is<object>(o => o.GetType().GetProperty("userId").GetValue(o, null).Equals(userId)), null, true, null, null))
                                   .Returns(new List<int> { totalSongs });

            _mockDatabaseOperations.Setup(c => c.Query<int>(It.IsAny<string>(), It.Is<object>(o => o.GetType().GetProperty("userId").GetValue(o, null).Equals(userId)), null, true, null, null))
                                   .Returns(new List<int> { mostListenedSongCount });

            var result = _repository.GetMostPlayedSongPercentile(userId);

            Assert.NotNull(result);
            Assert.Equal(mostPlayedSong, result.Item1);
            Assert.Equal(1, result.Item2);
        }

        [Fact]
        public void GetMostPlayedArtistPercentile_ShouldReturnCorrectPercentile()
        {
            var userId = 1;
            var mostPlayedArtistInfo = new MostPlayedArtistInformation { Artist_Id = 1, Start_Listen_Events = 5 };
            var mostPlayedArtist = "Most Played Artist";
            var totalSongs = 10;

            _mockDatabaseOperations.Setup(c => c.Query<MostPlayedArtistInformation>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(new List<MostPlayedArtistInformation> { mostPlayedArtistInfo });

            _mockDatabaseOperations.Setup(c => c.Query<string>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(new List<string> { mostPlayedArtist });

            _mockDatabaseOperations.Setup(c => c.Query<int>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                                   .Returns(new List<int> { totalSongs });

            var result = _repository.GetMostPlayedArtistPercentile(userId);

            Assert.NotNull(result);
            Assert.Equal(mostPlayedArtist, result.Item1);
            Assert.Equal(0.5m, result.Item2); 
        }

        [Fact]
        public void GetTop5Genres_ShouldReturnCorrectGenres()
        {
            var userId = 1;
            var expectedGenres = new List<string> { "Genre1", "Genre2", "Genre3", "Genre4", "Genre5" };

            _mockDatabaseOperations.Setup(c => c.Query<string>(It.IsAny<string>(), It.Is<object>(o => o.GetType().GetProperty("userId").GetValue(o, null).Equals(userId)), null, true, null, null))
                                   .Returns(expectedGenres);

            var result = _repository.GetTop5Genres(userId);

            Assert.NotNull(result);
            Assert.Equal(expectedGenres, result);
        }
        [Fact]
        public void GetAllNewGenresDiscovered_ShouldReturnCorrectNewGenres()
        {
            var userId = 1;
            var expectedNewGenres = new List<string> { "NewGenre1", "NewGenre2" };

            _mockDatabaseOperations.Setup(c => c.Query<string>(It.IsAny<string>(), It.Is<object>(o => o.GetType().GetProperty("userId").GetValue(o, null).Equals(userId)), null, true, null, null))
                                   .Returns(expectedNewGenres);

            var result = _repository.GetAllNewGenresDiscovered(userId);

            Assert.NotNull(result);
            Assert.Equal(expectedNewGenres, result);
        }
    }
}
