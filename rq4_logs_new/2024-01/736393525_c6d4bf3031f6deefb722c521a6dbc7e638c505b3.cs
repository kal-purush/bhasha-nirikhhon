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
    public class GenreRepositoryTests
    {
        [TestMethod]
        public async Task Repository_GetAllAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Genre>();
            var expectedGenres = TestUtilities.CreateGenres();

            // Act
            var actualGenres = await repository.GetAllAsync();

            // Assert
            Assert.IsTrue(expectedGenres.SequenceEqual(actualGenres, new GenreEqualityComparer()));
        }

        [TestMethod]
        public async Task Repository_GetByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Genre>();
            var expectedGenre = TestUtilities.CreateGenres().First();

            // Act
            var actualGenre = await repository.GetByIdAsync(expectedGenre.Id);

            // Assert
            Assert.IsNotNull(actualGenre);
            Assert.AreEqual(expectedGenre.Id, actualGenre.Id);
            Assert.AreEqual(expectedGenre.Name, actualGenre.Name);
        }

        [TestMethod]
        public async Task Repository_AddAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Genre>();
            var newGenre = new Genre { Name = "New Genre" };

            // Act
            await repository.AddAsync(newGenre);
            await repository.Context.SaveChangesAsync();
            var addedGenre = (await repository.GetAllAsync()).Last();

            // Assert
            Assert.IsNotNull(addedGenre);
            Assert.AreEqual(newGenre.Name, addedGenre.Name);
        }

        [TestMethod]
        public async Task Repository_UpdateAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Genre>();
            var existingGenre = (await repository.GetAllAsync()).First();

            existingGenre.Name = "Updated Genre";

            // Act
            var updatedGenre = await repository.UpdateAsync(existingGenre);
            await repository.Context.SaveChangesAsync();
            var retrievedUpdatedGenre = await repository.GetByIdAsync(existingGenre.Id);

            // Assert
            Assert.IsNotNull(updatedGenre);
            Assert.AreEqual(existingGenre.Id, retrievedUpdatedGenre.Id);
            Assert.AreEqual(existingGenre.Name, retrievedUpdatedGenre.Name);
        }

        [TestMethod]
        public async Task Repository_DeleteByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Genre>();
            var genres = await repository.GetAllAsync();
            var genreToDelete = genres.First();

            // Act
            var deletedGenre = await repository.DeleteByIdAsync(genreToDelete.Id);
            await repository.Context.SaveChangesAsync();
            var remainingGenres = await repository.GetAllAsync();

            // Assert
            Assert.IsNotNull(deletedGenre);
            Assert.AreEqual(genreToDelete.Id, deletedGenre.Id);
            CollectionAssert.DoesNotContain(remainingGenres.ToList(), genreToDelete);
        }
    }
}