using cursuri_studenti.book.model;
using cursuri_studenti.book.service;
using cursuri_studenti.student.model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace tests_cursuri_studenti.Tests
{
    public class TestBook
    {
        // Testing Book Service

        [Fact]
        public void FindById_ValidMatch_ReturnsBook()
        {
            // Arrange
            int Id = 123;
            Book expectedBook = new Book(Id, 123, "author", "name");
            List<Book> list = new List<Book> { expectedBook };
            BookService bookService = new BookService(list);

            // Act
            Book actualBook = bookService.FindById(Id);

            // Assert
            Assert.NotNull(actualBook);
            Assert.Equal(expectedBook.Id, actualBook.Id);
            Assert.Equal(expectedBook.StudentId, actualBook.StudentId);
            Assert.Equal(expectedBook.Author, actualBook.Author);
            Assert.Equal(expectedBook.Name, actualBook.Name);
        }

        [Fact] 
        public void FindById_NoMatch_ReturnsNull()
        {
            // Arrange
            int id = 123;
            List<Book> list = new List<Book>();
            BookService bookService = new BookService(list);

            // Act
            Book actualBook = bookService.FindById(id);

            // Assert
            Assert.Null(actualBook);
        }

        [Fact]
        public void FindById_MultipleBooks_ReturnsCorrectBook()
        {
            // Arrange
            int Id = 123;
            Book expectedBook = new Book(Id, 123, "author", "name");
            Book anotherBook = new Book(101, 100, "anotherauthor", "justaname");
            List<Book> list = new List<Book> { expectedBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            Book actualBook = bookService.FindById(Id);

            // Assert
            Assert.NotNull(actualBook);
            Assert.Equal(expectedBook.Id, actualBook.Id);
            Assert.Equal(expectedBook.StudentId, actualBook.StudentId);
            Assert.Equal(expectedBook.Author, actualBook.Author);
            Assert.Equal(expectedBook.Name, actualBook.Name);
        }

        [Fact]
        public void FindByName_ValidMatch_ReturnsBook()
        {
            // Arrange
            string Name = "name";
            Book expectedBook = new Book(123, 123, "author", Name);
            List<Book> list = new List<Book> { expectedBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByName(Name);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByName_NoMatch_ReturnsNull()
        {
            // Arrange
            string Name = "name";
            List<Book> list = new List<Book>();
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByName(Name);

            // Assert
            Assert.Empty(books);
        }

        [Fact]
        public void FindByName_MultipleBooks_ReturnsCorrectBook()
        {
            // Arrange
            string Name = "name";
            Book expectedBook = new Book(123, 123, "author", Name);
            Book anotherBook = new Book(101, 100, "anotherauthor", "justaname");
            List<Book> list = new List<Book> { expectedBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByName(Name);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByAuthor_ValidMatch_ReturnsBook()
        {
            // Arrange
            string Author = "author";
            Book expectedBook = new Book(123, 123, Author, "name");
            List<Book> list = new List<Book> { expectedBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthor(Author);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByAuthor_NoMatch_ReturnsNull()
        {
            // Arrange
            string Author = "author";
            List<Book> list = new List<Book>();
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthor(Author);

            // Assert
            Assert.Empty(books);
        }

        [Fact]
        public void FindByAuthor_MultipleBooks_ReturnsCorrectBook()
        {
            // Arrange
            string Author = "author";
            Book expectedBook = new Book(123, 123, Author, "name");
            Book anotherBook = new Book(101, 100, "anotherauthor", "justaname");
            List<Book> list = new List<Book> { expectedBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthor(Author);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByAuthorAndName_ValidMatch_ReturnsBook()
        {
            // Arrange
            string Author = "author", Name = "name";
            Book expectedBook = new Book(123, 123, Author, Name);
            List<Book> list = new List<Book> { expectedBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthorAndName(Author, Name);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByAuthorAndName_NoMatch_ReturnsNull()
        {
            // Arrange
            string Author = "author", Name = "name";
            List<Book> list = new List<Book>();
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthorAndName(Author, Name);

            // Assert
            Assert.Empty(books);
        }

        [Fact]
        public void FindByAuthorAndName_MultipleBooks_ReturnsCorrectBook()
        {
            // Arrange
            string Author = "author", Name = "name";
            Book expectedBook = new Book(123, 123, Author, Name);
            Book anotherBook = new Book(101, 100, "anotherauthor", "justaname");
            List<Book> list = new List<Book> { expectedBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByAuthorAndName(Author, Name);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByStudentId_ValidMatch_ReturnsBook()
        {
            // Arrange
            int StudentId = 123;
            Book expectedBook = new Book(123, StudentId, "author", "name");
            List<Book> list = new List<Book> { expectedBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByStudentId(StudentId);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void FindByStudentId_NoMatch_ReturnsNull()
        {
            // Arrange
            int StudentId = 123;
            List<Book> list = new List<Book>();
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByStudentId(StudentId);

            // Assert
            Assert.Empty(books);
        }

        [Fact]
        public void FindByStudentId_MultipleBooks_ReturnsCorrectBook()
        {
            // Arrange
            int StudentId = 123;
            Book expectedBook = new Book(123, StudentId, "author", "name");
            Book anotherBook = new Book(101, 100, "anotherauthor", "justaname");
            List<Book> list = new List<Book> { expectedBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            List<Book> books = bookService.FindByStudentId(StudentId);

            // Assert
            Assert.NotNull(books[0]);
            Assert.Equal(expectedBook.Id, books[0].Id);
            Assert.Equal(expectedBook.StudentId, books[0].StudentId);
            Assert.Equal(expectedBook.Author, books[0].Author);
            Assert.Equal(expectedBook.Name, books[0].Name);
        }

        [Fact]
        public void NewId_ReturnsUnusedId()
        {
            // Arrange
            int Id1 = 12, Id2 = 101;
            Book anBook = new Book(Id1, 111, "da2", "pa13231ss");
            Book anotherBook = new Book(Id2, 222, "fasf", "safas");
            List<Book> list = new List<Book> { anBook, anotherBook };
            BookService bookService = new BookService(list);

            // Act
            int newId = bookService.NewId();

            // Assert
            Assert.Null(bookService.FindById(newId));
            Assert.NotEqual(Id1, newId);
            Assert.NotEqual(Id2, newId);
            Assert.InRange(newId, 1, 9999);
        }

        [Fact]
        public void GetCount_ReturnsActualCount()
        {
            // Arrange
            Book anBook = new Book(123, 123, "authorName", "bookName");
            List<Book> list = new List<Book> { anBook };
            BookService bookService = new BookService(list);

            // Act
            int count = bookService.GetCount();

            // Assert
            Assert.Equal(list.Count(), count);
        }

        [Fact]
        public void ClearList_ListRemainsEmpty()
        {
            // Arrange
            Book anBook = new Book(123, 123, "authorName", "bookName");
            List<Book> list = new List<Book> { anBook };
            BookService bookService = new BookService(list);

            // Act
            bookService.ClearList();

            // Assert
            Assert.Equal(0, bookService.GetCount());
            Assert.Empty(bookService.GetList());
        }

        [Fact]
        public void GetList_ReturnsActualList()
        {
            // Arrange
            Book anBook = new Book(123, 123, "authorName", "bookName");
            List<Book> expectedList = new List<Book> { anBook };
            BookService bookService = new BookService(expectedList);

            // Act
            List<Book> actualList = bookService.GetList();

            // Assert
            Assert.Equal(expectedList, actualList);
            Assert.Equal(expectedList.Count(), actualList.Count());
        }

        [Fact]
        public void SetList_EditsList()
        {
            // Arrange
            Book anBook = new Book(123, 123, "authorName", "bookName");
            List<Book> list = new List<Book>();
            List<Book> setList = new List<Book> { anBook };
            BookService bookService = new BookService(list);

            // Act
            bookService.SetList(setList);

            // Assert
            Assert.Equal(setList, bookService.GetList());
            Assert.Equal(setList.Count(), bookService.GetCount());
            Assert.NotEqual(list, bookService.GetList());
        }
    }
}