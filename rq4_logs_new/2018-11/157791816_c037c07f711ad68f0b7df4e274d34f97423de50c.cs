using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibraryTerminal
{
    class ProgramApp
    {
        public static Dictionary<int, Book> SearchByAuthor(Dictionary<int, Book> bookList)
        {
            Console.Write("Enter author name: ");
            string userInput = Console.ReadLine();
            bool validAuthor = Validator.BookAuthorValidator(bookList, userInput);
            if (validAuthor == true)
            {
                int bookKey = BookApp.FindBookByAuthor(bookList, userInput);
                bool onShelf = BookApp.DisplayBook(bookList, bookKey);
                if (onShelf)
                {
                    Console.WriteLine("Would you like to check that book out? (y/n)");
                    if (Validator.YesNoAnswer())
                    {
                        bookList = BookApp.CheckOutBook(bookList, bookKey);
                    }
                }
            }
            return bookList;
        }

        public static Dictionary<int, Book> SearchByTitle(Dictionary<int, Book> bookList)
        {
            Console.Write("Enter title of the book: ");
            string userInput = Console.ReadLine();
            bool validTitle = Validator.BookTitleValidator(bookList, userInput);
            if (validTitle == true)
            {
                int bookKey = BookApp.FindBookByTitle(bookList, userInput);
                bool onShelf = BookApp.DisplayBook(bookList, bookKey);
                if (onShelf)
                {
                    Console.WriteLine("Would you like to check that book out? (y/n)");
                    if (Validator.YesNoAnswer())
                    {
                        bookList = BookApp.CheckOutBook(bookList, bookKey);
                    }
                }
            }
            return bookList;
        }

        public static Dictionary<int, Book> CheckOutBook(Dictionary<int, Book> bookList)
        {
            Console.WriteLine("How would you like to search for a book to check out? (title or author)");
            string userInput = Validator.DetermineTitleOrAuthor();
            bool validTitle;
            bool validAuthor;
            if (userInput == "title")
            {
                Console.Write("Enter title of the book: ");
                userInput = Console.ReadLine();
                if (validTitle = Validator.BookTitleValidator(bookList, userInput))
                {
                    int bookKey = BookApp.FindBookByTitle(bookList, userInput);
                    bookList = BookApp.CheckOutBook(bookList, bookKey);
                }
            }
            else if (userInput == "author")
            {
                Console.Write("Enter author name: ");
                userInput = Console.ReadLine();
                if (validAuthor = Validator.BookAuthorValidator(bookList, userInput))
                {
                    int bookKey = BookApp.FindBookByAuthor(bookList, userInput);
                    bookList = BookApp.CheckOutBook(bookList, bookKey);
                }
            }
            else { Console.WriteLine("Sorry, not valid"); }

            return bookList;
        }

        public static Dictionary<int, Book> ReturnBook(Dictionary<int, Book> bookList)
        {
            Console.WriteLine("How would you like to search for a book to return? (title or author)");
            string userInput = Validator.DetermineTitleOrAuthor();
            if (userInput == "title")
            {
                Console.Write("Enter title of the book: ");
                userInput = Console.ReadLine();
                int bookKey = BookApp.FindBookByTitle(bookList, userInput);
                bookList = BookApp.ReturnBook(bookList, bookKey);
            }
            else if (userInput == "author")
            {
                Console.Write("Enter author name: ");
                userInput = Console.ReadLine();
                int bookKey = BookApp.FindBookByAuthor(bookList, userInput);
                bookList = BookApp.ReturnBook(bookList, bookKey);
            }
            else { Console.WriteLine("Sorry, not valid"); }

            return bookList;
        }

    }
}