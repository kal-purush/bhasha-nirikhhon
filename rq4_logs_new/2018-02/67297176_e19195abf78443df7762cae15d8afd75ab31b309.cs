
namespace BooksCore.Provider
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Collections.ObjectModel;
    using BooksCore.Books;
    using BooksCore.Geography;
    using BooksCore.Interfaces;

    public class BooksReadProvider : IBooksReadProvider
    {
        private Dictionary<string, WorldCountry> _worldCountryLookup;

        private IGeographyProvider _geographyProvider;

        /// <summary>
        /// Gets the books.
        /// </summary>
        public ObservableCollection<BookRead> BooksRead { get; private set; }

        /// <summary>
        /// Gets the books.
        /// </summary>
        public ObservableCollection<BookAuthor> AuthorsRead { get; private set; }

        /// <summary>
        /// Gets the books.
        /// </summary>
        public ObservableCollection<BookLocationDelta> BookLocationDeltas { get; private set; }

        /// <summary>
        /// Gets the books.
        /// </summary>
        public ObservableCollection<AuthorCountry> AuthorCountries { get; private set; }

        private void UpdateCountries(out int booksReadWorldwide, out uint pagesReadWorldwide)
        {
            // clear the list & counts
            AuthorCountries.Clear();
            booksReadWorldwide = 0;
            pagesReadWorldwide = 0;

            // get the uniquely named countries + the counts
            Dictionary<string, AuthorCountry> countrySet = new Dictionary<string, AuthorCountry>();
            foreach (BookAuthor author in AuthorsRead)
            {
                booksReadWorldwide += author.TotalBooksReadBy;
                pagesReadWorldwide += author.TotalPages;

                if (countrySet.ContainsKey(author.Nationality))
                    countrySet[author.Nationality].AuthorsFromCountry.Add(author);
                else
                {
                    AuthorCountry country = new AuthorCountry(_geographyProvider) { Country = author.Nationality };
                    country.AuthorsFromCountry.Add(author);
                    countrySet.Add(country.Country, country);
                }
            }

            // Update the country totals + add to the list
            foreach (var country in countrySet.Values.ToList())
            {
                country.TotalBooksWorldWide = booksReadWorldwide;
                country.TotalPagesWorldWide = pagesReadWorldwide;
                AuthorCountries.Add(country);
            }
        }

        private void UpdateBookLocationDeltas()
        {
            // clear the list and the counts
            BookLocationDeltas.Clear();
            if (BooksRead.Count < 1) return;
            DateTime startDate = BooksRead[0].Date;

            // get all the dates a book has been read (after the first quarter)
            Dictionary<DateTime, DateTime> bookReadDates = GetBookReadDates(startDate);

            // then add the delta made up of the books up to that date
            foreach (DateTime date in bookReadDates.Keys.ToList())
            {
                BookLocationDelta delta = new BookLocationDelta(date, startDate);
                foreach (var book in BooksRead)
                {
                    if (book.Date <= date)
                    {
                        WorldCountry country = GetCountryForBook(book);
                        if (country != null)
                        {
                            BookLocation location =
                                new BookLocation() { Book = book, Latitude = country.Latitude, Longitude = country.Longitude };

                            delta.BooksLocationsToDate.Add(location);
                        }
                    }
                    else
                        break;
                }
                //delta.UpdateTallies();
                BookLocationDeltas.Add(delta);
            }
        }
        private WorldCountry GetCountryForBook(BookRead book)
        {
            if (_worldCountryLookup == null || _worldCountryLookup.Count == 0 ||
                !_worldCountryLookup.ContainsKey(book.Nationality)) return null;
            return _worldCountryLookup[book.Nationality];
        }

        private Dictionary<DateTime, DateTime> GetBookReadDates(DateTime startDate)
        {
            Dictionary<DateTime, DateTime> bookReadDates = new Dictionary<DateTime, DateTime>();
            foreach (var book in BooksRead)
            {
                if (!bookReadDates.ContainsKey(book.Date))
                {
                    TimeSpan ts = book.Date - startDate;
                    if (ts.Days >= 90)
                        bookReadDates.Add(book.Date, book.Date);
                }
            }
            return bookReadDates;
        }

        private void UpdateAuthors()
        {
            AuthorsRead.Clear();

            Dictionary<string, BookAuthor> authorsSet = new Dictionary<string, BookAuthor>();
            foreach (BookRead book in BooksRead)
            {
                if (authorsSet.ContainsKey(book.Author))
                    authorsSet[book.Author].BooksReadBy.Add(book);
                else
                {
                    BookAuthor author =
                        new BookAuthor() { Author = book.Author, Language = book.OriginalLanguage, Nationality = book.Nationality };
                    author.BooksReadBy.Add(book);
                    authorsSet.Add(book.Author, author);
                }
            }

            foreach (var author in authorsSet.Values.ToList())
                AuthorsRead.Add(author);
        }

        private void UpdateWorldCountryLookup()
        {
            _worldCountryLookup = new Dictionary<string, WorldCountry>();
            foreach (var country in _geographyProvider.WorldCountries)
                if (!_worldCountryLookup.ContainsKey(country.Country))
                    _worldCountryLookup.Add(country.Country, country);
        }

        public void Setup(IList<BookRead> books, IGeographyProvider geographyProvider)
        {
            _geographyProvider = geographyProvider;

            BooksRead.Clear();
            foreach (var book in books)
                BooksRead.Add(book);

            UpdateAuthors();
            UpdateWorldCountryLookup();
            int booksReadWorldwide;
            uint pagesReadWorldwide;
            UpdateCountries(out booksReadWorldwide, out pagesReadWorldwide);
            BookLocationDeltas = new ObservableCollection<BookLocationDelta>();
            UpdateBookLocationDeltas();
        }

        public BooksReadProvider()
        {
            BooksRead = new ObservableCollection<BookRead>();
            AuthorsRead = new ObservableCollection<BookAuthor>();
            BookLocationDeltas = new ObservableCollection<BookLocationDelta>();
            AuthorCountries = new ObservableCollection<AuthorCountry>();
        }
    }
}