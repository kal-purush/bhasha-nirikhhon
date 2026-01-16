using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Virtual_librarian
{
    class ServiceToLibrary
    {
        public static LibraryObjects.Person PersonToLibraryObject(PersonService.Person personFromPersonService)
        {
            return new LibraryObjects.Person(personFromPersonService.Id, personFromPersonService.Name, personFromPersonService.Surname, personFromPersonService.Password, personFromPersonService.BirthDate, personFromPersonService.PhoneNumber, personFromPersonService.Email);
        }
        /*
        public static PersonService.Person PersonToServiceObject(LibraryObjects.Person personFromLibrary)
        {
            return main(personFromLibrary.Id, personFromLibrary.Name, personFromLibrary.Surname, personFromLibrary.Password, personFromLibrary.BirthDate, personFromLibrary.PhoneNumber, personFromLibrary.Email);
        }
        /*
        public static LibraryObjects.Person PersonToServiceObject(PersonService.Person personFromPersonService)
        {
            return new LibraryObjects.Person(personFromPersonService.Id, personFromPersonService.Name, personFromPersonService.Surname, personFromPersonService.Password, personFromPersonService.BirthDate, personFromPersonService.PhoneNumber, personFromPersonService.Email);
        }
        public static BookService.Person PersonToServiceObject(LibraryObjects.Person personFromBookService)
        {
            return new BookService.Person(personFromBookService.Id, personFromBookService.Name, personFromBookService.Surname, personFromBookService.Password, personFromBookService.BirthDate, personFromBookService.PhoneNumber, personFromBookService.Email);
        }
        */

        /*
        public delegate LibraryObjects.Book Delegate(BookService.Book bookFromService);
        public static Delegate bookToLibraryObjectDel = new Delegate(BookToLibraryObject);
        */

        public static LibraryObjects.Book BookToLibraryObject(BookService.Book bookFromBookService)
        {
            return new LibraryObjects.Book(bookFromBookService.Id, bookFromBookService.Name, bookFromBookService.Author, bookFromBookService.Publisher, bookFromBookService.Year, bookFromBookService.Pages, bookFromBookService.Isbn, bookFromBookService.Code, bookFromBookService.IsTaken, bookFromBookService.ReaderId, bookFromBookService.TakenAt, bookFromBookService.ReturnAt);
        }

        public static List<LibraryObjects.Book> BookListToLibraryObject(List<BookService.Book> bookListFromBookService)
        {
            List<LibraryObjects.Book> bookList = new List<LibraryObjects.Book>();

            foreach (BookService.Book bookFromService in bookListFromBookService)
            {
                bookList.Add(BookToLibraryObject(bookFromService));
            }

            return bookList;
        }
    }
}