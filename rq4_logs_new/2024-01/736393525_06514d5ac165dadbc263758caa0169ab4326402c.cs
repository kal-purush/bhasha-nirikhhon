using BusinessLogic.Interfaces;
using BusinessLogic.Models;
using BusinessLogic.Services;
using BusinessLogic.Validation;
using Moq;

namespace OnlineLibriary.Tests.BusinessLogic.Tests.Services.Tests
{
    [TestClass]
    public class AuthenticationServiceTests
    {
        private AuthenticationService CreateService()
        {
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var mapper = TestUtilities.CreateMapper();
            var service = new AuthenticationService(validator, unitOfWork, mapper);
            return service;
        }

        [TestMethod]
        public async Task SignIn_WithValidData_ShouldReturnValidResult()
        {
            // Arrange
            var service = CreateService();
            var userCredentials = new UserCredentials { Username = "Username1", Password = "Password1" };

            // Act
            var result = await service.SignIn(userCredentials);

            // Assert
            Assert.IsTrue(result.IsSuccessful);
            Assert.IsNull(result.ErrorMessage);
            Assert.IsNotNull(result.User);
            Assert.AreEqual("Username1", result.User.Username);
        }

        [TestMethod]
        public async Task SignIn_WithEmptyUsername_ShouldReturnInvalidResult()
        {
            // Arrange
            var service = CreateService();
            var userCredentials = new UserCredentials { Username = "", Password = "Password1" };

            // Act
            var result = await service.SignIn(userCredentials);

            // Assert
            Assert.IsFalse(result.IsSuccessful);
            Assert.IsNotNull(result.ErrorMessage);
            Assert.IsNull(result.User);
            Assert.AreEqual("Username or password is incorrect", result.ErrorMessage);
        }

        [TestMethod]
        public async Task SignIn_WithEmptyPassword_ShouldReturnInvalidResult()
        {
            // Arrange
            var service = CreateService();
            var userCredentials = new UserCredentials { Username = "Username1" };

            // Act
            var result = await service.SignIn(userCredentials);

            // Assert
            Assert.IsFalse(result.IsSuccessful);
            Assert.IsNotNull(result.ErrorMessage);
            Assert.IsNull(result.User);
            Assert.AreEqual("Username or password is incorrect", result.ErrorMessage);
        }

        [TestMethod]
        public async Task SignIn_WithInvalidUsername_ShouldReturnInvalidResult()
        {
            // Arrange
            var service = CreateService();
            var userCredentials = new UserCredentials { Username = "Username3", Password = "Password1" };

            // Act
            var result = await service.SignIn(userCredentials);

            // Assert
            Assert.IsFalse(result.IsSuccessful);
            Assert.IsNotNull(result.ErrorMessage);
            Assert.IsNull(result.User);
            Assert.AreEqual("Username or password is incorrect", result.ErrorMessage);
        }
    }
}