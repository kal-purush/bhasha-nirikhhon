using API.Models;
using BusinessLogic.Models;
using OnlineLibriary.Tests.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineLibriary.Tests.Integration.Tests
{
    [TestClass]
    public class AuthenticationControllerTests
    {
        private readonly APIBuilder _apiBuilder;
        public AuthenticationControllerTests()
        {
            _apiBuilder = new APIBuilder();
        }

        [TestMethod]
        public async Task Signin_WithValidData_ShouldReturnValidResult()
        {
            // Arrange
            var signInModel = new UserCredentials { Username = "Username1", Password = "Password1" };

            // Act
            var response = await _apiBuilder.PostRequestWithDeserializing<Token>("api/signin", signInModel);

            // Assert
            Assert.IsNotNull(response);
            Assert.IsTrue(!string.IsNullOrEmpty(response.AccessToken));
            Assert.IsNotNull(response.ExpiresIn);
        }

        [TestMethod]
        public async Task Signin_WithInvalidData_ShouldReturnValidResult()
        {
            // Arrange
            var signInModel = new UserCredentials { Username = "Username1", Password = "Password2" };   
            var expectedMessage = "Username or password is incorrect";

            // Act
            var response = await _apiBuilder.PostRequest("api/signin", signInModel);
            var actualMessage = await response.Content.ReadAsStringAsync();
            // Assert
            Assert.AreEqual(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
            Assert.AreEqual(expectedMessage, actualMessage);
        }
    }
}