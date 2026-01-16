namespace BookCatalog.Core.Models
{
    public class Library<T> where T : Book
    {
        public Catalog<T> Catalog { get; }
        public List<string> PressReleaseItems { get; private set; }

        public Library(Catalog<T> catalog)
        {
            Catalog = catalog;
            PressReleaseItems = new List<string>();
            PopulatePressReleaseItems(catalog);
        }

        private void PopulatePressReleaseItems(Catalog<T> catalog)
        {
            if (typeof(T).Name.Equals(typeof(EBook).Name))
            {
                var eBooks = Catalog.Books.Select(x => x.Value as EBook).ToList();

                foreach (var book in eBooks)
                {
                    PressReleaseItems.AddRange(book!.AvailableFormats.Where(x => !PressReleaseItems.Contains(x)));
                }
            }
            else if (typeof(T).Name.Equals(typeof(PaperBook).Name))
            {
                PressReleaseItems.AddRange(Catalog.Books.Select(x => x.Value as PaperBook).Where(x => !PressReleaseItems.Contains(x!.Publisher)).Select(book => book!.Publisher));
            }
        }
    }
}