using FluentAssertions;
using Moq;
using NUnit.Framework;
using SFA.DAS.EmployerRequestApprenticeTraining.Application.Commands.CacheStandard;
using SFA.DAS.EmployerRequestApprenticeTraining.Domain.Interfaces;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Requests;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Responses;
using SFA.DAS.Testing.AutoFixture;

namespace SFA.DAS.EmployerRequestApprenticeTraining.Application.UnitTests.Commands
{
    [TestFixture]
    public class WhenHandlingCacheStandardCommand
    {
        private Mock<IEmployerRequestApprenticeTrainingOuterApi> _mockOuterApi;
        private CacheStandardCommandHandler _handler;
        private CacheStandardCommand _command;
        private readonly string standardLarsCode = "12345";

        [SetUp]
        public void Setup()
        {
            _mockOuterApi = new Mock<IEmployerRequestApprenticeTrainingOuterApi>();
            _handler = new CacheStandardCommandHandler(_mockOuterApi.Object);
            _command = new CacheStandardCommand(standardLarsCode);
        }

        [Test, MoqAutoData]
        public async Task Handle_ShouldCallOuterApiWithCorrectParameters_AndReturnStandard(Standard standard)
        {
            // Arrange
            _mockOuterApi.Setup(x => x.CacheStandard(
                    It.Is<string>(p => p == _command.StandardLarsCode)))
                .ReturnsAsync(standard);

            // Act
            var result = await _handler.Handle(_command, CancellationToken.None);

            // Assert
            result.Should().BeEquivalentTo(standard);
            _mockOuterApi.Verify(x => x.CacheStandard(standardLarsCode), Times.Once);
        }

        [Test]
        public void Handle_WhenApiThrowsException_ShouldRethrowIt()
        {
            // Arrange
            _mockOuterApi.Setup(x => x.CacheStandard(It.IsAny<string>()))
                .ThrowsAsync(new Exception("API error"));

            // Act
            Func<Task> act = async () => await _handler.Handle(_command, CancellationToken.None);

            // Assert
            act.Should().ThrowAsync<Exception>().WithMessage("API error");
        }
    }
}
