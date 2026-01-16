using Host.Core.Entities;
using Host.Core.Interfaces.Repositories;
using Host.Core.Interfaces.Services;

namespace Host.BLL
{
    public class BookService : IBookService
    {
        private readonly IBookRepository _bookRepository;

        public BookService(IBookRepository bookRepository)
        {
            _bookRepository = bookRepository;
        }

        public async Task<Book> Get(Guid id)
        {
            var book  = _bookRepository.Get(id);

            if (book == null)
            {
                throw new Exception($"Book with Id {id} could not be found");
            }

            await Task.CompletedTask;
            return book;

        }

        public async Task<IEnumerable<Book>> Get()
        {
            var books = _bookRepository.Get();
            return books;
        }

        public async Task CreateAsync(Book book)
        {
            book.Id = Guid.NewGuid();
            _bookRepository.CreateAsync(book);
        }

        public void UpdateAsync(Book book)
        {
            _bookRepository.UpdateAsync(book);
        }

        public async Task DeleteAsync(Guid id)
        {
            var book = _bookRepository.Get(id);

            if (book == null)
            {
                throw new Exception($"Book with Id {id} could not be found");
            }

            _bookRepository.DeleteAsync(book);
        }
    }
}