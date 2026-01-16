using DAL;
using System;
using System.Collections.Generic;

namespace BL
{
    public class BooksService : IBooksService
    {
        private readonly IBooksRepository _booksRepository;

        public BooksService(IBooksRepository booksRepository)
        {
            _booksRepository = booksRepository;
        }
        public Guid AddBook(Book book)
        {
            ValidateBookState(book);

            return _booksRepository.Add(book);
        }

        public IEnumerable<Book> GetAllBooks()
        {
            return _booksRepository.GetAll();
        }

        public IEnumerable<Book> GetBookByAuthor(string author)
        {
            return _booksRepository.GetByAuthor(author);
        }

        public Book GetBookById(Guid id)
        {
            return _booksRepository.GetById(id);
        }

        public bool RemoveBookById(Guid id)
        {
            return _booksRepository.RemoveById(id);
        }

        public bool UpdateBook(Book book)
        {
            ValidateBookState(book);

            return _booksRepository.Update(book);
        }

        private void ValidateBookState(Book book)
        {
            if (book.PageCount < 10 || book.PageCount > 2000)
            {
                throw new ArgumentException("Ivalid pages count");
            }
        }
    }
}