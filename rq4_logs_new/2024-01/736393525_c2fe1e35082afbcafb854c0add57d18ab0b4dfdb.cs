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
    public class BookRepositoryTests
    {
        [TestMethod]
        public async Task Repository_GetAllAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Book>();
            var expectedBooks = TestUtilities.CreateBooks();

            // Act
            var actualBooks = await repository.GetAllAsync();

            // Assert
            Assert.IsTrue(expectedBooks.SequenceEqual(actualBooks, new BookEqualityComparer()));
        }

        [TestMethod]
        public async Task Repository_GetByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Book>();
            var expectedBook = TestUtilities.CreateBooks().First();

            // Act
            var actualBook = await repository.GetByIdAsync(expectedBook.Id);

            // Assert
            Assert.IsNotNull(actualBook);
            Assert.AreEqual(expectedBook.Id, actualBook.Id);
            Assert.AreEqual(expectedBook.Title, actualBook.Title);
            Assert.AreEqual(expectedBook.Description, actualBook.Description);
            Assert.AreEqual(expectedBook.Year, actualBook.Year);
        }

        [TestMethod]
        public async Task Repository_AddAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Book>();
            var newBook = new Book
            {
                Title = "New Book",
                Description = "Description of the new book",
                Year = 2023,
                AuthorId = 1,
                GenreId = 1,
                BookContent = new byte[0]
            };

            // Act
            await repository.AddAsync(newBook);
            await repository.Context.SaveChangesAsync();
            var addedBook = (await repository.GetAllAsync()).Last();

            // Assert
            Assert.IsNotNull(addedBook);
            Assert.AreEqual(newBook.Title, addedBook.Title);
            Assert.AreEqual(newBook.Description, addedBook.Description);
            Assert.AreEqual(newBook.Year, addedBook.Year);
        }

        [TestMethod]
        public async Task Repository_UpdateAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Book>();
            var existingBook = (await repository.GetAllAsync()).First();

            existingBook.Title = "Updated Title";

            // Act
            var updatedBook = await repository.UpdateAsync(existingBook);
            await repository.Context.SaveChangesAsync();
            var retrievedUpdatedBook = await repository.GetByIdAsync(existingBook.Id);

            // Assert
            Assert.IsNotNull(updatedBook);
            Assert.AreEqual(existingBook.Id, retrievedUpdatedBook.Id);
            Assert.AreEqual(existingBook.Title, retrievedUpdatedBook.Title);
        }

        [TestMethod]
        public async Task Repository_DeleteByIdAsync()
        {
            // Arrange
            var repository = TestUtilities.CreateRepository<Book>();
            var books = await repository.GetAllAsync();
            var bookToDelete = books.First();

            // Act
            var deletedBook = await repository.DeleteByIdAsync(bookToDelete.Id);
            await repository.Context.SaveChangesAsync();
            var remainingBooks = await repository.GetAllAsync();

            // Assert
            Assert.IsNotNull(deletedBook);
            Assert.AreEqual(bookToDelete.Id, deletedBook.Id);
            CollectionAssert.DoesNotContain(remainingBooks.ToList(), bookToDelete);
        }
    }
}