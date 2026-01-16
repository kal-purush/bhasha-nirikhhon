using EasyTravel.Application.Services;
using EasyTravel.Domain.Entites;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EasyTravel.Test.ApplicationTests.Services
{
    [TestFixture]
    public class RegisterServiceTests
    {
        private Mock<UserManager<User>> _userManagerMock = null!;
        private Mock<ILogger<RegisterService>> _loggerMock = null!;
        private RegisterService _registerService = null!;

        [SetUp]
        public void SetUp()
        {
            _userManagerMock = MockUserManager();
            _loggerMock = new Mock<ILogger<RegisterService>>();
            _registerService = new RegisterService(_userManagerMock.Object, _loggerMock.Object);
        }

        private Mock<UserManager<User>> MockUserManager()
        {
            var store = new Mock<IUserStore<User>>();
            return new Mock<UserManager<User>>(
                store.Object,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        }

        [Test]
        public void RegisterUserAsync_ShouldThrowArgumentNullException_WhenUserIsNull()
        {
            // Act & Assert
            var ex = Assert.ThrowsAsync<ArgumentNullException>(() => _registerService.RegisterUserAsync(null!, "password"));
            Assert.That(ex!.ParamName, Is.EqualTo("user"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to register a null User entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task RegisterUserAsync_ShouldReturnError_WhenPasswordIsEmpty()
        {
            // Arrange
            var user = new User
            {
                Id = Guid.NewGuid(),
                Email = "test@example.com"
            };

            // Act
            var result = await _registerService.RegisterUserAsync(user, string.Empty);

            // Assert
            Assert.Multiple(() =>
            {
                Assert.That(result.Success, Is.False);
                Assert.That(result.ErrorMessage, Is.EqualTo("Password cannot be empty."));
            });

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to register a user with an empty password.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task RegisterUserAsync_ShouldReturnSuccess_WhenRegistrationIsSuccessful()
        {
            // Arrange
            var user = new User
            {
                Id = Guid.NewGuid(),
                Email = "test@example.com"
            };

            _userManagerMock.Setup(u => u.CreateAsync(user, "password"))
                .ReturnsAsync(IdentityResult.Success);
            _userManagerMock.Setup(u => u.AddToRoleAsync(user, "client"))
                .ReturnsAsync(IdentityResult.Success);

            // Act
            var result = await _registerService.RegisterUserAsync(user, "password");

            // Assert
            Assert.Multiple(() =>
            {
                Assert.That(result.Success, Is.True);
                Assert.That(result.ErrorMessage, Is.Empty);
            });

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully registered user with email: {user.Email}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task RegisterUserAsync_ShouldReturnError_WhenRegistrationFails()
        {
            // Arrange
            var user = new User
            {
                Id = Guid.NewGuid(),
                Email = "test@example.com"
            };

            var identityErrors = new List<IdentityError>
            {
                new IdentityError { Description = "Password is too weak." },
                new IdentityError { Description = "Email is already taken." }
            };

            _userManagerMock.Setup(u => u.CreateAsync(user, "password"))
                .ReturnsAsync(IdentityResult.Failed(identityErrors.ToArray()));

            // Act
            var result = await _registerService.RegisterUserAsync(user, "password");

            // Assert
            Assert.Multiple(() =>
            {
                Assert.That(result.Success, Is.False);
                Assert.That(result.ErrorMessage, Is.EqualTo("Password is too weak. Email is already taken."));
            });

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Failed to register user with email: {user.Email}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void RegisterUserAsync_ShouldThrowInvalidOperationException_WhenExceptionOccurs()
        {
            // Arrange
            var user = new User
            {
                Id = Guid.NewGuid(),
                Email = "test@example.com"
            };

            _userManagerMock.Setup(u => u.CreateAsync(user, "password"))
                .ThrowsAsync(new Exception("Database connection failed."));

            // Act
            var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _registerService.RegisterUserAsync(user, "password"));

            // Assert
            Assert.Multiple(() =>
            {
                Assert.That(ex, Is.Not.Null);
                Assert.That(ex!.Message, Is.EqualTo($"An error occurred while registering the user with email: {user.Email}."));
                Assert.That(ex.InnerException, Is.TypeOf<Exception>());
                Assert.That(ex.InnerException?.Message, Is.EqualTo("Database connection failed."));
            });

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"An error occurred while registering the user with email: {user.Email}")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }
}

