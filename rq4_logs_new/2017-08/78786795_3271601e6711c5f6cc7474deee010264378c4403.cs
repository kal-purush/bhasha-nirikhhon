using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FDM90.Handlers;
using Moq;
using FDM90.Repository;
using FDM90.Models;
using System.Collections.Generic;

namespace FDM90UnitTests
{
    [TestClass]
    public class SchedulerHandlerTests
    {
        private Mock<IRepository<ScheduledPost>> _schedulerRepoMock;
        private Mock<IFacebookHandler> _facebookHandlerMock;
        private Mock<ITwitterHandler> _twitterHandlerMock;
        private Mock<IUserHandler> _userHandlerMock;
        private SchedulerHandler _schedulerHandler;

        [TestInitialize]
        public void StartUp()
        {
            _schedulerRepoMock = new Mock<IRepository<ScheduledPost>>();
            _schedulerRepoMock.As<IReadMultipleSpecific<ScheduledPost>>();

            _facebookHandlerMock = new Mock<IFacebookHandler>();
            _facebookHandlerMock.As<IMediaHandler>();
            _facebookHandlerMock.Setup(p => p.MediaName).Returns("Facebook");
            _facebookHandlerMock.As<IMediaHandler>().Setup(p => p.PostData(It.IsAny<Dictionary<string, string>>(), It.IsAny<Guid>())).Verifiable();

            _twitterHandlerMock = new Mock<ITwitterHandler>();
            _twitterHandlerMock.As<IMediaHandler>();
            _twitterHandlerMock.Setup(p => p.MediaName).Returns("Twitter");
            _twitterHandlerMock.As<IMediaHandler>().Setup(p => p.PostData(It.IsAny<Dictionary<string, string>>(), It.IsAny<Guid>())).Verifiable();

            _userHandlerMock = new Mock<IUserHandler>();

            _schedulerHandler = new SchedulerHandler(_schedulerRepoMock.Object, _facebookHandlerMock.Object, _twitterHandlerMock.Object, _userHandlerMock.Object);
        }

        [TestMethod]
        public void SchedulerPostsForTime_GivenPostToSingleChannel_ReturnsTrueIfOneHandlerPostCalled()
        {
            // arrange
            User testUser = new User()
            {
                UserId = Guid.NewGuid(),
                Facebook = true,
                Twitter = true
            };

            ScheduledPost post = new ScheduledPost()
            {
                PostId = Guid.NewGuid(),
                UserId = testUser.UserId,
                PostText = "Test Post",
                MediaChannels = "Facebook"
            };

            _schedulerRepoMock.As<IReadMultipleSpecific<ScheduledPost>>().Setup(s => s.ReadMultipleSpecific(It.IsAny<string>())).Returns(new List<ScheduledPost>() { post });
            _userHandlerMock.Setup(s => s.GetUser(It.IsAny<string>())).Returns(testUser);

            // act
            _schedulerHandler.SchedulerPostsForTime(new DateTime());

            // assert
            _facebookHandlerMock.As<IMediaHandler>().Verify(p => p.PostData(It.IsAny<Dictionary<string, string>>(), It.IsAny<Guid>()), Times.Once);
            _twitterHandlerMock.As<IMediaHandler>().Verify(p => p.PostData(It.IsAny<Dictionary<string, string>>(), It.IsAny<Guid>()), Times.Never);
        }
    }
}