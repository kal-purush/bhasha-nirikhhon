using Data.Entities;
using OnlineLibriaryTests.Utils.Compareres;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineLibriaryTests.Data.Tests
{
    [TestClass]
    public class UserRepositoryTests
    {
        [TestMethod]
        public async Task Repository_GetAllAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<User>();
            var expectedUsers = TestUtilities.CreateUsers();

            // Act
            var actualUsers = await repository.GetAllAsync();

            // Assert
            Assert.IsTrue(expectedUsers.SequenceEqual(actualUsers, new UserEqualityComparer()));
        }

        [TestMethod]
        public async Task Repository_GetByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<User>();
            var expectedUser = TestUtilities.CreateUsers().First();

            // Act
            var actualUser = await repository.GetByIdAsync(expectedUser.Id);

            // Assert
            Assert.IsNotNull(actualUser);
            Assert.AreEqual(expectedUser.Id, actualUser.Id);
            Assert.AreEqual(expectedUser.FirstName, actualUser.FirstName);
            Assert.AreEqual(expectedUser.LastName, actualUser.LastName);
            Assert.AreEqual(expectedUser.DateOfRegistration.ToString("dd.MM.yyyy"), actualUser.DateOfRegistration.ToString("dd.MM.yyyy"));
            Assert.AreEqual(expectedUser.Email, actualUser.Email);
        }

        [TestMethod]
        public async Task Repository_AddAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<User>();
            var newUser = new User
            {
                FirstName = "New",
                LastName = "User",
                DateOfRegistration = DateTime.Now,
                Email = "newuser@example.com",
                Password = "password123",
                ProfilePicture = new byte[0]
            };

            // Act
            await repository.AddAsync(newUser);
            await repository.Context.SaveChangesAsync();
            var addedUser = (await repository.GetAllAsync()).Last();

            // Assert
            Assert.IsNotNull(addedUser);
            Assert.AreEqual(newUser.FirstName, addedUser.FirstName);
            Assert.AreEqual(newUser.LastName, addedUser.LastName);
            Assert.AreEqual(newUser.DateOfRegistration.ToString("dd.MM.yyyy"), addedUser.DateOfRegistration.ToString("dd.MM.yyyy"));
            Assert.AreEqual(newUser.Email, addedUser.Email);
        }

        [TestMethod]
        public async Task Repository_UpdateAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<User>();
            var existingUser = (await repository.GetAllAsync()).First();

            existingUser.FirstName = "Updated";

            // Act
            var updatedUser = await repository.UpdateAsync(existingUser);
            await repository.Context.SaveChangesAsync();
            var retrievedUpdatedUser = await repository.GetByIdAsync(existingUser.Id);

            // Assert
            Assert.IsNotNull(updatedUser);
            Assert.AreEqual(existingUser.Id, retrievedUpdatedUser.Id);
            Assert.AreEqual(existingUser.FirstName, retrievedUpdatedUser.FirstName);
        }

        [TestMethod]
        public async Task Repository_DeleteByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<User>();
            var users = await repository.GetAllAsync();
            var userToDelete = users.First();

            // Act
            var deletedUser = await repository.DeleteByIdAsync(userToDelete.Id);
            await repository.Context.SaveChangesAsync();
            var remainingUsers = await repository.GetAllAsync();

            // Assert
            Assert.IsNotNull(deletedUser);
            Assert.AreEqual(userToDelete.Id, deletedUser.Id);
            CollectionAssert.DoesNotContain(remainingUsers.ToList(), userToDelete);
        }

    }
}