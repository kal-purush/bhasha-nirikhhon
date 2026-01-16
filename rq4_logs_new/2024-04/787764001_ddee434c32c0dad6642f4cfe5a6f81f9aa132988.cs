using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Xunit;
using TechTitans.Repositories;
using TechTitans.Models;

namespace TechTitansTesting.Repositories
{
    public class UserPlaybackBehaviourRepositoryTest
    {
        private Mock<IDatabaseOperations> _mockDatabaseOperations;
        private UserPlaybackBehaviourRepository _repository;

        [Fact]
        public void GetUserPlaybackBehaviour_WhenUserPlaybackBehaviourExists_ShouldReturnUserPlaybackBehaviour()
        {
            _mockDatabaseOperations = new Mock<IDatabaseOperations>();
            _repository = new UserPlaybackBehaviourRepository(_mockDatabaseOperations.Object);
            var expectedUserPlaybackBehaviour = new UserPlaybackBehaviour 
            { 
                User_Id = 1, 
                Song_Id = 1, 
                Event_Type = TechTitans.Enums.PlaybackEventType.startSongPlayback, 
                Timestamp = DateTime.Now 
            };
            _mockDatabaseOperations.Setup(c => c.Query<UserPlaybackBehaviour>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                           .Returns(new List<UserPlaybackBehaviour> { expectedUserPlaybackBehaviour });

            var result = _repository.GetUserPlaybackBehaviour(1, 1, DateTime.Now);

            Assert.NotNull(result);
            Assert.Equal(expectedUserPlaybackBehaviour.User_Id, result.User_Id);
        }

        [Fact]
        public void GetUserPlaybackBehaviour_ShouldReturnListOfUserPlaybackBehaviour()
        {
            _mockDatabaseOperations = new Mock<IDatabaseOperations>();
            _repository = new UserPlaybackBehaviourRepository(_mockDatabaseOperations.Object);
            var expectedUserPlaybackBehaviour = new List<UserPlaybackBehaviour> ()
            {
                new UserPlaybackBehaviour()
                {
                    User_Id = 1, 
                    Song_Id = 1, 
                    Event_Type = TechTitans.Enums.PlaybackEventType.startSongPlayback, 
                    Timestamp = DateTime.Now 
                },
                new UserPlaybackBehaviour()
                {
                    User_Id = 1, 
                    Song_Id = 2, 
                    Event_Type = TechTitans.Enums.PlaybackEventType.startSongPlayback, 
                    Timestamp = DateTime.Now 
                }
                
            };
            _mockDatabaseOperations.Setup(c => c.Query<UserPlaybackBehaviour>(It.IsAny<string>(), It.IsAny<object>(), null, true, null, null))
                           .Returns(expectedUserPlaybackBehaviour);

            var result = _repository.GetUserPlaybackBehaviour(1);

            Assert.NotNull(result);
            Assert.Equal(2, result.Count());
        }
    }

}