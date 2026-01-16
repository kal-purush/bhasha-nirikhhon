using BusinessLogic.Models;
using BusinessLogic.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineLibriary.Tests.BusinessLogic.Tests.Validation.Tests
{
    [TestClass]
    public class AuthenticationValidatorTests
    {
        [TestMethod]
        public async Task ValidateForSignInAsync_WithValidData_ShouldReturnValidResult()
        {
            // Arrange
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var userCredentials = new UserCredentials { Username = "Username1", Password = "Password1" };

            // Act
            var result = await validator.ValidateForSignInAsync(userCredentials);

            // Assert
            Assert.IsTrue(result.IsValid);
            Assert.IsFalse(result.Messages.Any());
        }

        [TestMethod]
        public async Task ValidateForSignInAsync_WithEmptyUsername_ShouldReturnInvalidResult()
        {
            // Arrange
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var userCredentials = new UserCredentials { Username = "", Password = "Password1" };

            // Act
            var result = await validator.ValidateForSignInAsync(userCredentials);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.IsTrue(result.Messages.Any());
            Assert.AreEqual(1, result.Messages.Count);
            Assert.IsTrue(result.Messages.Contains("Username or password is incorrect"));
        }

        [TestMethod]
        public async Task ValidateForSignInAsync_WithEmptyPassword_ShouldReturnInvalidResult()
        {
            // Arrange
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var userCredentials = new UserCredentials { Username = "Username1" };

            // Act
            var result = await validator.ValidateForSignInAsync(userCredentials);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.IsTrue(result.Messages.Any());
            Assert.AreEqual(1, result.Messages.Count);
            Assert.IsTrue(result.Messages.Contains("Username or password is incorrect"));
        }

        [TestMethod]
        public async Task ValidateForSignInAsync_WithNonExistingUsername_ShouldReturnInvalidResult()
        {
            // Arrange
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var userCredentials = new UserCredentials { Username = "Username3", Password = "Password1" };

            // Act
            var result = await validator.ValidateForSignInAsync(userCredentials);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.IsTrue(result.Messages.Any());
            Assert.AreEqual(1, result.Messages.Count);
            Assert.IsTrue(result.Messages.Contains("Username or password is incorrect"));
        }

        [TestMethod]
        public async Task ValidateForSignInAsync_WithIncorrectPassword_ShouldReturnInvalidResult()
        {
            // Arrange
            var unitOfWork = TestUtilities.CreateUnitOfWork();
            var validator = new AuthenticationValidator(unitOfWork, TestUtilities.CreateSecurePasswordHasher());
            var userCredentials = new UserCredentials { Username = "Username1", Password = "Password3" };

            // Act
            var result = await validator.ValidateForSignInAsync(userCredentials);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.IsTrue(result.Messages.Any());
            Assert.AreEqual(1, result.Messages.Count);
            Assert.IsTrue(result.Messages.Contains("Username or password is incorrect"));
        }
    }
}