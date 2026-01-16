using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinalProjectLibraryManagerV01E.Models.Managers
{
    internal class BookManager
    {
        private List<Book> books = new List<Book>();

        public void AddBook(Book book)
        {
            books.Add(book);
        }

        public List<Book> GetAllBooks()
        {
            return books;
        }

        public Book GetBookByTitle(string title)
        {
            return books.Find(book => book.Title == title);
        }

        public void UpdateBook(Book updatedBook)
        {
            var index = books.FindIndex(book => book.Title == updatedBook.Title);
            if (index != -1)
            {
                books[index] = updatedBook;
            }
        }

        public void RemoveBook(Book book)
        {
            books.Remove(book);
        }        
    }
}