using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BookingSystem
{
   static class  TempController
    {
        public static bool addBook(string id, string title, string author, string category, string year)
        {
            Book book = new Book(id,title,author,category,year);
            
            try
            {
                //TODO: send book to DAL. 
                    return true;
            }
            catch (Exception e)
            {
                //Console output is temporary********
                Console.WriteLine("Error Message: " + e.Source);
                return false;
            }
        }

        public static bool deleteBook(string id)
        {
            try
            {
                //TODO: send id to DAL. 
                return true;
            }
            catch (Exception e)
            {
                //Console output is temporary********
                Console.WriteLine("Error Message: " + e.Source);
                return false;
            }
        }
        public static List<Book> getBooks()
        {
            Book b1 = new Book("id3", "title3", "author3", "category3", "year3");
            Book b2 = new Book("id4", "title4", "author4", "category4", "year4");
            List<Book> b = new List<Book>();
            b.Add(b1);
            b.Add(b2);
            //TODO: Get bookList from DAL

            // and  return bookList;
            return b;
        }
        public static List<Book> getBooks(string[] args)
        {
            Book b1 = new Book("id", "title", "author", "category", "year");
            Book b2 = new Book("id2", "title2", "author2", "category2", "year2");
            List<Book> b = new List<Book>();
            //TODO: Get List of requested books
            b.Add(b1);
            b.Add(b2);
            // and return requestedBooks;
            return b;
        }

        public static void testing()
        {
            string[] s = { "a", "b", "c" };


            

            foreach (Book book in getBooks()) 
            {
                Console.WriteLine("inuti getBooks()");
                Console.WriteLine("{0}, {1}, {2}, {3}, {4}", book.id, book.title, book.author, book.category, book.year);
            }

            foreach (Book book in getBooks(s))
            {
                Console.WriteLine("inuti getBooks(s)");
                Console.WriteLine(book.id + book.title + book.author + book.category + book.year);
            }

        }
    }
}