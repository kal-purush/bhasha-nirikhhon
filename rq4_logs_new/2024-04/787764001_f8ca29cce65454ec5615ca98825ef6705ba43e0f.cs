using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTitans.Models;
using TechTitans.Repositories;
using TechTitans.Services;
using Moq;
using TechTitansTesting.Services.Stubs;
using Microsoft.Extensions.Hosting;
using Xunit;
using TechTitans.Enums;

namespace TechTitansTesting.Services
{

    public class FullDetailsOnSongControllerTest
    {
        private readonly TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour> userPlaybackBehaviourRepoMock;
        private readonly TestFullDetailsOnSongControllerRepository<AdDistributionData> adDistributionDataRepoMock;

        [Fact]
        public void GetFullDetailsOnSong_GetSongThatExists_returnsCorrectData()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1).AddMinutes(1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Like, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Dislike, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Skip, Timestamp = DateTime.Now.AddDays(-1) }
            };


            var adDistributionDataMock = new AdDistributionData
            {
                Song_Id = songId,
                Month = DateTime.Now.Month,
                Year = DateTime.Now.Year,
                Ad_Campaign = 101,
                Genre = "Rock",
                Language = "English"
            };

            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );


            var result = controller.GetFullDetailsOnSong(songId);

            Assert.NotNull(result);
            Assert.Equal(1, result.TotalPlays);
            Assert.Equal(1, result.TotalLikes);
            Assert.Equal(1, result.TotalDislikes);
            Assert.Equal(1, result.TotalSkips);
            Assert.Equal(1, result.TotalMinutesListened);
        }

        [Fact]
        public void GetFullDetailsOnSong_GetStartSongData_returnsCorrectStartingTimes()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
            };


            var adDistributionDataMock = new AdDistributionData
            { };

            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );


            var result = controller.GetFullDetailsOnSong(songId);

            Assert.NotNull(result);
            Assert.Equal(4, result.TotalPlays);
            Assert.Equal(0, result.TotalLikes);
            Assert.Equal(0, result.TotalDislikes);
            Assert.Equal(0, result.TotalSkips);
            Assert.Equal(0, result.TotalMinutesListened);
        }

        [Fact]
        public void GetFullDetailsOnSong_SongWhichNotExists_ReturnsNull()
        {
            int songId = 2;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            { };

            var adDistributionDataMock = new AdDistributionData
            {
            };

            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );


            var result = controller.GetFullDetailsOnSong(songId);

            Assert.Null(result);
        }


        [Fact]
        public void GetCurrentMonthDetails_GetDetailsOfCurrentMonthSong_AggregatesPlaybackBehaviorDataForCurrentMonth()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {   
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.EndSongPlayback, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Like, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Dislike, Timestamp = DateTime.Now.AddDays(-1) },
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.Skip, Timestamp = DateTime.Now.AddDays(-1) }
            };

            var adDistributionDataMock = new AdDistributionData
            { };
            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );

            var result = controller.GetCurrentMonthDetails(songId);

            Assert.NotNull(result);
            Assert.Equal(1, result.TotalPlays);
            Assert.Equal(1, result.TotalLikes);
            Assert.Equal(1, result.TotalDislikes);
            Assert.Equal(1, result.TotalSkips);
            Assert.Equal(0, result.TotalMinutesListened);
        }

        [Fact]
        public void GetCurrentMonthDetails_GetDetailsForSongNotFromCurrentMonth_ReturnsDefaultSongDetails()
        {
            int songId = 1;

            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {
                new UserPlaybackBehaviour { Song_Id = songId, Event_Type = PlaybackEventType.StartSongPlayback, Timestamp = DateTime.Now.AddMonths(-1) },
            };

            var adDistributionDataMock = new AdDistributionData
            { };

            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );
            var result = controller.GetCurrentMonthDetails(songId);

            Assert.NotNull(result);
            Assert.Equal(0, result.TotalPlays);
            Assert.Equal(0, result.TotalLikes);
            Assert.Equal(0, result.TotalDislikes);
            Assert.Equal(0, result.TotalSkips);
            Assert.Equal(0, result.TotalMinutesListened);
        }

        [Fact]
        public void GetCurrentMonthDetails_GetSongWhichNotExists_ReturnsDefaultSongDetails()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {
            };

            var adDistributionDataMock = new AdDistributionData
            {
            };
            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );
            var result = controller.GetCurrentMonthDetails(songId);

            Assert.NotNull(result);
            Assert.Equal(0, result.TotalPlays);
            Assert.Equal(0, result.TotalLikes);
            Assert.Equal(0, result.TotalDislikes);
            Assert.Equal(0, result.TotalSkips);
            Assert.Equal(0, result.TotalMinutesListened);
        }


        [Fact]
        public void GetActiveAd_GetCurrentAd_AggregatesAdDataForCurrentMonth()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            { };

            var adDistributionDataMock = new AdDistributionData
            {
                Song_Id = songId,
                Month = DateTime.Now.Month,
                Year = DateTime.Now.Year,
                Ad_Campaign = 101,
                Genre = "Rock",
                Language = "English"
            };
            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );

            var result = controller.GetActiveAd(songId);

            Assert.NotNull(result);
            Assert.Equal(songId, result.Song_Id);
            Assert.Equal(DateTime.Now.Month, result.Month);
            Assert.Equal(DateTime.Now.Year, result.Year);
            Assert.Equal(101, result.Ad_Campaign);
            Assert.Equal("Rock", result.Genre);
            Assert.Equal("English", result.Language);
        }

        [Fact]
        public void GetActiveAd_GetAdWithSongThatNotExists_ReturnsNull()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            { };

            var adDistributionDataMock = new AdDistributionData
            {
                Song_Id = 2,
                Month = DateTime.Now.AddMonths(-1).Month,
                Year = DateTime.Now.Year,
                Ad_Campaign = 101,
                Genre = "Rock",
                Language = "English"
            };
            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );
            var result = controller.GetActiveAd(songId);

            Assert.Null(result);
        }

        [Fact]
        public void GetActiveAd_GetAdOfSongFromEmptyAdList_ReturnsNull()
        {
            int songId = 1;
            var userPlaybackBehaviourData = new List<UserPlaybackBehaviour>
            {
            };

            var adDistributionDataMock = new AdDistributionData
            {
            };
            var adDistributionData = new List<AdDistributionData> { adDistributionDataMock };

            var userPlaybackBehaviourRepositoryStub = new TestFullDetailsOnSongControllerRepository<UserPlaybackBehaviour>(userPlaybackBehaviourData);
            var adDistributionDataRepositoryStub = new TestFullDetailsOnSongControllerRepository<AdDistributionData>(adDistributionData);

            var controller = new FullDetailsOnSongController(
                userPlaybackBehaviourRepositoryStub,
                adDistributionDataRepositoryStub
            );
            var result = controller.GetActiveAd(songId);

            Assert.Null(result);
        }
    }
}