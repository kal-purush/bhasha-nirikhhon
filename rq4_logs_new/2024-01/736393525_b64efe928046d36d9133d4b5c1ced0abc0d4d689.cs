using Data.Data;
using Data.Entities;
using Data.Repositories;
using Microsoft.EntityFrameworkCore;
using OnlineLibriaryTests.Utils.Compareres;

namespace OnlineLibriaryTests.Data.Tests
{
    [TestClass]
    public class AuthorRepositoryTests
    {        
        [TestMethod]
        public async Task Repository_GetAllAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Author>();
            var expectedAuthors = TestUtilities.CreateAuthors();
            // Act
            var actualAuthors = await repository.GetAllAsync();

            // Assert
            Assert.IsTrue(expectedAuthors.SequenceEqual(actualAuthors, new AuthorEqualityComparer()));
        }

        [TestMethod]
        public async Task Repository_GetByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Author>();
            var expectedAuthor = TestUtilities.CreateAuthors().First();

            // Act
            var actualAuthor = await repository.GetByIdAsync(expectedAuthor.Id);

            // Assert
            Assert.IsNotNull(actualAuthor);
            Assert.AreEqual(expectedAuthor.Id, actualAuthor.Id);
            Assert.AreEqual(expectedAuthor.FirstName, actualAuthor.FirstName);
            Assert.AreEqual(expectedAuthor.LastName, actualAuthor.LastName);
            Assert.AreEqual(expectedAuthor.DateOfBirth.ToString("dd.MM.yyyy"), actualAuthor.DateOfBirth.ToString("dd.MM.yyyy"));
        }

        [TestMethod]
        public async Task Repository_AddAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Author>();
            var newAuthor = new Author { FirstName = "New", LastName = "Author", DateOfBirth = DateTime.Now, Country = "Country11"};

            // Act
            await repository.AddAsync(newAuthor);
            await repository.Context.SaveChangesAsync();
            var addedAuthor = (await repository.GetAllAsync()).Last();

            // Assert
            Assert.IsNotNull(addedAuthor);
            Assert.AreEqual(newAuthor.Id, addedAuthor.Id);
            Assert.AreEqual(newAuthor.FirstName, addedAuthor.FirstName);
            Assert.AreEqual(newAuthor.LastName, addedAuthor.LastName);
            Assert.AreEqual(newAuthor.DateOfBirth.ToString("dd.MM.yyyy"), addedAuthor.DateOfBirth.ToString("dd.MM.yyyy"));
        }

        [TestMethod]
        public async Task Repository_UpdateAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Author>();
            var existingAuthor = (await repository.GetAllAsync()).First();

            existingAuthor.FirstName = "Updated";

            // Act
            var updatedAuthor = await repository.UpdateAsync(existingAuthor);
            await repository.Context.SaveChangesAsync();
            var retrievedUpdatedAuthor = await repository.GetByIdAsync(existingAuthor.Id);

            // Assert
            Assert.IsNotNull(updatedAuthor);
            Assert.AreEqual(existingAuthor.Id, retrievedUpdatedAuthor.Id);
            Assert.AreEqual(existingAuthor.FirstName, retrievedUpdatedAuthor.FirstName);
        }

        [TestMethod]
        public async Task Repository_DeleteByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Author>();
            var authors = await repository.GetAllAsync();
            var authorToDelete = authors.First(); // Get an author for deletion

            // Act
            var deletedAuthor = await repository.DeleteByIdAsync(authorToDelete.Id);
            await repository.Context.SaveChangesAsync();
            var remainingAuthors = await repository.GetAllAsync();

            // Assert
            Assert.IsNotNull(deletedAuthor);
            Assert.AreEqual(authorToDelete.Id, deletedAuthor.Id);
            CollectionAssert.DoesNotContain(remainingAuthors.ToList(), authorToDelete);
        }
    }
}