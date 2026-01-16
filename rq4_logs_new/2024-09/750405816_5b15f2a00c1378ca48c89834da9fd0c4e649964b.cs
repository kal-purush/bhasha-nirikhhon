using FluentAssertions;
using FluentValidation;
using FluentValidation.Results;
using Moq;
using NUnit.Framework;
using SFA.DAS.EmployerRequestApprenticeTraining.Application.Queries.GetExistingEmployerRequest;
using SFA.DAS.EmployerRequestApprenticeTraining.Domain.Interfaces;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Responses;

namespace SFA.DAS.EmployerRequestApprenticeTraining.Application.UnitTests.Queries.GetExistingEmployerRequest
{
    [TestFixture]
    public class GetExistingEmployerRequestQueryTests
    {
        private Mock<IEmployerRequestApprenticeTrainingOuterApi> _outerApiMock;
        private Mock<IValidator<GetExistingEmployerRequestQuery>> _mockValidator;
        private GetExistingEmployerRequestQueryHandler _handler;

        [SetUp]
        public void Setup()
        {
            _outerApiMock = new Mock<IEmployerRequestApprenticeTrainingOuterApi>();
            _mockValidator = new Mock<IValidator<GetExistingEmployerRequestQuery>>();
            _handler = new GetExistingEmployerRequestQueryHandler(_outerApiMock.Object, _mockValidator.Object);
        }

        [Test]
        public async Task Handle_Should_CallValidator()
        {
            // Arrange
            var query = new GetExistingEmployerRequestQuery { AccountId = 12345, StandardReference = "ST0001" };
            _mockValidator.Setup(v => v.ValidateAsync(query, It.IsAny<CancellationToken>())).ReturnsAsync(new ValidationResult());

            // Act
            await _handler.Handle(query, CancellationToken.None);

            // Assert
            _mockValidator.Verify(v => v.ValidateAsync(query, It.IsAny<CancellationToken>()), Times.Once);
        }


        [Test]
        public async Task Handle_Should_ReturnEmployerRequest_When_AccountId_And_StandardReference_AreProvided()
        {
            // Arrange
            var accountId = 123;
            var standardReference = "ST0123";
            var query = new GetExistingEmployerRequestQuery { AccountId = accountId, StandardReference = standardReference };

            _mockValidator.Setup(v => v.ValidateAsync(query, It.IsAny<CancellationToken>())).ReturnsAsync(new ValidationResult());
            _outerApiMock.Setup(api => api.GetExistingEmployerRequest(accountId, standardReference)).ReturnsAsync(true);

            // Act
            var result = await _handler.Handle(query, CancellationToken.None);

            // Assert
            result.Should().BeTrue();
        }

        [Test]
        public void Handle_Should_ThrowValidationException_When_ValidationFails()
        {
            // Arrange
            var query = new GetExistingEmployerRequestQuery();
            var validationResult = new ValidationResult();

            _mockValidator
                .Setup(v => v.ValidateAsync(query, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ValidationException(new[] { new ValidationFailure("Property", "Error") } ));

            // Act & Assert
            Assert.ThrowsAsync<ValidationException>(() => _handler.Handle(query, CancellationToken.None));
        }
    }
}