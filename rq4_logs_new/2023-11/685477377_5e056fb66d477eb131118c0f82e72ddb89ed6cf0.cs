using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Moq;

namespace AvSBookStore.Tests
{
    public class BookServiceTests
    {
        [Fact]
        public void GetAllByQuery_WithIsbn_CallsGetAllByIsbn()
        {

            Moq.Mock<IBookRepository> bookRepositoryStub = new Mock<IBookRepository>();

            bookRepositoryStub.Setup(x => x.getAllByIsbn(It.IsAny<string>()))
                .Returns(new[] { new Book(1, "", "", "") });

            bookRepositoryStub.Setup(x => x.getAllByTitleOrAuthor(It.IsAny<string>()))
                .Returns(new[] { new Book(2, "", "", "") });

            BookService bookService = new BookService(bookRepositoryStub.Object);

            string validIsbn = "ISBN 12345-57890";

            Book[] actual = bookService.GetAllByQuery(validIsbn);

            Assert.Collection(actual, book => Assert.Equal(1, book.Id));
        }

        [Fact]
        public void GetAllByQuery_WithAuthor_CallsGetAllByTitleOrAuthor()
        {
             
            Moq.Mock<IBookRepository> bookRepositoryStub = new Mock<IBookRepository>();

            bookRepositoryStub.Setup(x => x.getAllByIsbn(It.IsAny<string>()))
                .Returns(new[] { new Book(1, "", "", "") });

            bookRepositoryStub.Setup(x => x.getAllByTitleOrAuthor(It.IsAny<string>()))
                .Returns(new[] { new Book(2, "", "", "") });

            BookService bookService = new BookService(bookRepositoryStub.Object);

            string invalidIsbn = "12345-57890";

            Book[] actual = bookService.GetAllByQuery(invalidIsbn);

            Assert.Collection(actual, book => Assert.Equal(2, book.Id));
        }
    }
}