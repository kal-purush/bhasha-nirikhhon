using System;
using System.Threading.Tasks;

namespace ProlificLibrary
{
    public class BookEditViewModel
    {
        readonly IResource resource = new RemoteResource();
        public Book Book { get; private set; }

        public string Title { get; set; }
        public string Author { get; set; }
        public string Categories { get; set; }
        public string Publisher { get; set; }

        public BookEditViewModel(Book book = null)
        {
            this.Book = book;
        }

        public async Task AddBook() 
        {
            if (HasValidData()) {
                var newBook = new Book(Title, Author, Categories, Publisher);
                Book = await resource.AddBook(newBook);
            } else {
                throw new Exception("Title and author fields are required");
            }
        }

        bool HasValidData() 
        {
            return IsTextValid(Title) && IsTextValid(Author);
        }

        bool IsTextValid(string text)
        {
            var trimmedText = text.Trim();
            return trimmedText.Length > 0;
        }
    }
}