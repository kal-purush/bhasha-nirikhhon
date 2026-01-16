using DAL.Entities;
using DAL.Repositories;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BL.Services
{
    public class BooksService : IBooksService
    {
        private readonly IGerericRepository<Book> _booksRepository;

        public BooksService(IGerericRepository<Book> booksRepository)
        {
            _booksRepository = booksRepository;
        }
        public async Task<Guid> AddBook(Book book)
        {
            ValidateBookState(book);

            return await _booksRepository.Add(book);
        }

        public async Task<bool> RemoveBookById(Guid id)
        {
            return await _booksRepository.RemoveById(id);
        }

        public async Task<IEnumerable<Book>> GetAllBooks()
        {
            return await _booksRepository.GetAll();
        }

        public async Task<Book> GetBookById(Guid id)
        {
            return await _booksRepository.GetById(id);
        }

        public async Task<bool> UpdateBook(Book book)
        {
            ValidateBookState(book);

            return await _booksRepository.Update(book);
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