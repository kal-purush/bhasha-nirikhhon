using BookCatalog.Core.Interfaces;
using BookCatalog.Core.Models;

namespace BookCatalog.Core.AbstractFactory.LibrariesFactory
{
    public class LibraryFactory : LibraryAbstractFactory
    {
        private readonly ICsvRepository _repository;

        public LibraryFactory(ICsvRepository repository)
        {
            _repository = repository;
        }

        public override async Task<Library<EBook>> LoadEBookLibrary()
        {
            return await _repository.LoadEBooks();
        }

        public override async Task<Library<PaperBook>> LoadPaperBookLibrary()
        {
            return await _repository.LoadPaperBooks();
        }
    }
}