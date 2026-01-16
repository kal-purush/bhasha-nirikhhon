using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Models;
using TechTitans.Repositories;
using TechTitans.Services;

namespace TechTitansTesting.Services
{
    public class TopGenresControllerTest
    {
        private Mock<IRepository<SongDataBaseModel>> songRepoMock;
        private Mock<IRepository<SongRecommendationDetails>> songRecommendationRepoMock;
        private TopGenresController _controller;

        public TopGenresControllerTest()
        {
            songRepoMock = new Mock<IRepository<SongDataBaseModel>>();
            songRecommendationRepoMock = new Mock<IRepository<SongRecommendationDetails>>();
            songRepoMock.Setup(repo => repo.GetAll()).Returns(GetExpectedSongs());
            songRecommendationRepoMock.Setup(repo => repo.GetAll()).Returns(GetExpectedRecommendationSongs());
            _controller = new TopGenresController(songRepoMock.Object, songRecommendationRepoMock.Object);
        }

        [Fact]
        public void GetTop3Genres_SetsGenreNameAndMinutesListenedAndPercentageOfLabelsForTop3Genres()
        {
            var genre1 = new Label();
            var minutes1 = new Label();
            var percentage1 = new Label();
            var genre2 = new Label();
            var minutes2 = new Label();
            var percentage2 = new Label();
            var genre3 = new Label();
            var minutes3 = new Label();
            var percentage3 = new Label();
            _controller.GetTop3Genres(8, 2015, genre1, minutes1, percentage1, genre2, minutes2, percentage2, genre3, minutes3, percentage3);
            Assert.Equal("Rock", genre1.Text);
            Assert.Equal("4520000", minutes1.Text);
            Assert.Equal("67", percentage1.Text);
            Assert.Equal("Pop", genre2.Text);
            Assert.Equal("880000", minutes2.Text);
            Assert.Equal("13", percentage2.Text);
            Assert.Equal("Hip Hop", genre3.Text);
            Assert.Equal("720000", minutes3.Text);
            Assert.Equal("10", percentage3.Text);
        }

        [Fact]
        public void GetTop3SubGenres_SetsGenreNameAndMinutesListenedAndPercentageOfLabelsForTop3Genres()
        {
            var subgenre1 = new Label();
            var minutes1 = new Label();
            var percentage1 = new Label();
            var subgenre2 = new Label();
            var minutes2 = new Label();
            var percentage2 = new Label();
            var subgenre3 = new Label();
            var minutes3 = new Label();
            var percentage3 = new Label();
            _controller.GetTop3SubGenres(8, 2015, subgenre1, minutes1, percentage1, subgenre2, minutes2, percentage2, subgenre3, minutes3, percentage3);
            Assert.Equal("Hard Rock", subgenre1.Text);
            Assert.Equal("4520000", minutes1.Text);
            Assert.Equal("67", percentage1.Text);
            Assert.Equal("Dance", subgenre2.Text);
            Assert.Equal("880000", minutes2.Text);
            Assert.Equal("13", percentage2.Text);
            Assert.Equal("Rap", subgenre3.Text);
            Assert.Equal("720000", minutes3.Text);
            Assert.Equal("10", percentage3.Text);
        }

        private List<SongDataBaseModel> GetExpectedSongs()
        {
            return new List<SongDataBaseModel>
            {
                new SongDataBaseModel
                {
                    Song_Id = 1,
                    Name = "Song 1",
                    Genre = "Pop",
                    Subgenre = "Dance",
                    Artist_Id = 101,
                    Language = "English",
                    Country = "USA",
                    Album = "Album 1",
                    Image = "song1_img.png"
                },
                new SongDataBaseModel
                {
                    Song_Id = 2,
                    Name = "Song 2",
                    Genre = "Rock",
                    Subgenre = "Hard Rock",
                    Artist_Id = 102,
                    Language = "English",
                    Country = "Canada",
                    Album = "Album 2",
                    Image = "song2_img.png"
                },
                new SongDataBaseModel
                {
                    Song_Id = 3,
                    Name = "Song 3",
                    Genre = "Jazz",
                    Subgenre = "Smooth Jazz",
                    Artist_Id = 103,
                    Language = "English",
                    Country = "UK",
                    Album = "Album 3",
                    Image = "song3_img.png"
                },
                new SongDataBaseModel
                {
                    Song_Id = 4,
                    Name = "Song 4",
                    Genre = "Classical",
                    Subgenre = "Symphony",
                    Artist_Id = 104,
                    Language = "English",
                    Country = "Germany",
                    Album = "Album 4",
                    Image = "song4_img.png"
                },
                new SongDataBaseModel
                {
                    Song_Id = 5,
                    Name = "Song 5",
                    Genre = "Hip Hop",
                    Subgenre = "Rap",
                    Artist_Id = 105,
                    Language = "English",
                    Country = "USA",
                    Album = "Album 5",
                    Image = "song5_img.png"
                },
                new SongDataBaseModel
                {
                    Song_Id = 6,
                    Name = "Song 6",
                    Genre = "Electronic",
                    Subgenre = "Techno",
                    Artist_Id = 106,
                    Language = "English",
                    Country = "Australia",
                    Album = "Album 6",
                    Image = "song6_img.png"
                }
            };
        }

        private List<SongRecommendationDetails> GetExpectedRecommendationSongs()
        {
            return new List<SongRecommendationDetails>
            {
                new SongRecommendationDetails
                {
                    Song_Id = 4,
                    Likes = 5000,
                    Dislikes = 360,
                    Minutes_Listened = 15000,
                    Number_Of_Plays = 270,
                    Month = 5,
                    Year = 2010
                },
                new SongRecommendationDetails
                {
                    Song_Id = 2,
                    Likes = 25000,
                    Dislikes = 6000,
                    Minutes_Listened = 120000,
                    Number_Of_Plays = 43000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 3,
                    Likes = 52000,
                    Dislikes = 24000,
                    Minutes_Listened = 560000,
                    Number_Of_Plays = 108000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 1,
                    Likes = 178000,
                    Dislikes = 98000,
                    Minutes_Listened = 880000,
                    Number_Of_Plays = 340000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 10,
                    Likes = 218000,
                    Dislikes = 108000,
                    Minutes_Listened = 1090000,
                    Number_Of_Plays = 670000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 5,
                    Likes = 88000,
                    Dislikes = 48000,
                    Minutes_Listened = 720000,
                    Number_Of_Plays = 240000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 2,
                    Likes = 2550000,
                    Dislikes = 1060000,
                    Minutes_Listened = 4400000,
                    Number_Of_Plays = 5320000,
                    Month = 8,
                    Year = 2015
                },
                new SongRecommendationDetails
                {
                    Song_Id = 5,
                    Likes = 212000,
                    Dislikes = 66000,
                    Minutes_Listened = 320000,
                    Number_Of_Plays = 365000,
                    Month = 8,
                    Year = 2012
                },
            };
        }
    }
}