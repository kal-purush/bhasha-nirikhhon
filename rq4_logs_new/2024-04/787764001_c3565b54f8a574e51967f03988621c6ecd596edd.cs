using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Repositories;
using TechTitans.Models;
using Moq;
using Xunit;

namespace TechTitansTesting.Repositories
{
    public class UserSongRepositoryTest
    {
        private Mock<IDatabaseOperations> _mockDatabaseOperations;
        private UserSongRepository _repository;

        public UserSongRepositoryTest()
        {
            _mockDatabaseOperations = new Mock<IDatabaseOperations>();
            _repository = new UserSongRepository(_mockDatabaseOperations.Object);
        }

        [Fact]
        public void ConvertSongDataBaseModelToSongInfo_ShouldReturnSongBasicInfoWithArtistName()
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

            var result = _repository.ConvertSongDataBaseModelToSongInfo(songBasicDetails);

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
    }
}