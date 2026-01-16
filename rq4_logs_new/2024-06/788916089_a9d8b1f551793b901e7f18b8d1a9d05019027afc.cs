using BookCatalog.Core.Interfaces;
using BookCatalog.Core.Models;

namespace BookCatalog.Core.AbstractFactory.LibrariesFactory
{
    public class EBookFactory : BookFactory
    {
        public EBookFactory(ICsvRepository repository)
            : base(repository) { }

        public override async Task<ICatalog> CreateCatalogAsync()
        {
            var catalog = await _repository.LoadEBooks();

            return catalog;
        }

        public override List<string> CreatePressReleaseItems(ICatalog catalog)
        {
            var pressReleaseItems = new List<string>();
            var eBooks = catalog.Books.Values.Cast<EBook>().ToList();

            foreach (var book in eBooks)
            {
                foreach (var format in book.AvailableFormats)
                {
                    if (!pressReleaseItems.Contains(format))
                    {
                        pressReleaseItems.Add(format);
                    }
                }
            }

            return pressReleaseItems;
        }
    }
}