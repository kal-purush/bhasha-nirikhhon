using System;
using System.Threading.Tasks;

namespace ProlificLibrary
{
    public class BookDetailsViewModel
    {
        readonly Book book;
        readonly IResource resource = new RemoteResource();

        public BookDetailsViewModel(Book book)
        {
            this.book = book;
        }

        public String Title {
            get { return book.title; }
        }

        public String Author {
            get { return book.author; }
        }

        public String Categories {
            get { return book.categories; }
        }

        public String Publisher {
            get { return book.publisher; }
        }

        public async Task<Book> CheckoutBook()
        {
            return await resource.GetBook(book.id);
        }
    }
}