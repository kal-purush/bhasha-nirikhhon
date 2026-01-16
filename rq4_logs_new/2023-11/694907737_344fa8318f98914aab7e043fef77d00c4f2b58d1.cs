/** 
 * AUTHOR: Isaac Hillaker
 * DATE: 11/02/2023
 * LAST UPDATED: 11/02/2023
 *
 * NOTES: This file contains unit tests for the User model class. Specifically it is testing to ensure that the name, email, and password fields are validating user input properly.
 *
 */

using System.ComponentModel.DataAnnotations;
using DnD_NPC_Generator.Models;

namespace DnD_UnitTests
{
    public class UserTests
    {
        /*===============
         Test VALID input
        ===============*/
        [Fact]
        public void ValidUser()
        {
            // Arrange
            var user = new User
            {
                UserId = 1,
                Name = "John Doe",
                Email = "john.doe@example.com",
                Password = "StrongPassword123!"
            };

            // Act
            var validationResults = new List<ValidationResult>();
            var isValid = Validator.TryValidateObject(user, new ValidationContext(user), validationResults, true);

            // Assert
            Assert.True(isValid);
            Assert.Empty(validationResults);
        }

        /*===============
         Test INVALID input
        ===============*/
        [Theory]
        [InlineData("", "john.doe@example.com", "StrongPassword123!", "Please enter a Name.")]
        [InlineData("John Doe", "invalidemail", "WeakPwd", "Please enter a valid email address.", "Password must be at least 8 characters.", "Password must meet complexity requirements.")]
        public void InvalidUser(string name, string email, string password, params string[] expectedErrorMessages)
        {
            // Arrange
            var user = new User
            {
                UserId = 1,
                Name = name,
                Email = email,
                Password = password
            };

            // Act
            var validationResults = new List<ValidationResult>();
            var isValid = Validator.TryValidateObject(user, new ValidationContext(user), validationResults, true);

            // Assert
            Assert.False(isValid);
            Assert.Equal(expectedErrorMessages.Length, validationResults.Count);

            foreach (var expectedErrorMessage in expectedErrorMessages)
            {
                Assert.Contains(validationResults, result => result.ErrorMessage == expectedErrorMessage);
            }
        }
    }
}