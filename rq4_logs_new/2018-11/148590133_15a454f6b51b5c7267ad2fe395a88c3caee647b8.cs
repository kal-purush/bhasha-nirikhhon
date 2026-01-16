using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using LibraryObjects;
using Database;

/// <summary>
/// Summary description for BookService
/// </summary>
[WebService(Namespace = "http://tempuri.org/")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
// [System.Web.Script.Services.ScriptService]
public class BookService : System.Web.Services.WebService
{
    BookDBHelper bookDBHelper = new BookDBHelper();

    public BookService()
    {
        //Uncomment the following line if using designed components 
        //InitializeComponent(); 
    }

    [WebMethod]
    public string HelloWorld()
    {
        return "Hello World";
    }

    [WebMethod]
    public Book GetBookById(int id)
    {
        return bookDBHelper.GetBookById(id);
    }

    [WebMethod]
    public Book GetBookByCode(string isbn, string code)
    {
        return bookDBHelper.GetBookByCode(isbn, code);
    }

    [WebMethod]
    public Book GetBookByIsbn(string isbn)
    {
        return bookDBHelper.GetBookByIsbn(isbn);
    }

    [WebMethod]
    public List<Book> GetAllBooks()
    {
        return bookDBHelper.GetAllBooks();
    }

    [WebMethod]
    public List<Book> GetReadersBooks(Person reader)
    {
        return bookDBHelper.GetReadersBooks(reader);
    }

    [WebMethod]
    public bool ReturnBook(Book returnedBook)
    {
        return bookDBHelper.ReturnBook(returnedBook);
    }

    [WebMethod]
    public List<Book> Find(string search)
    {
        return bookDBHelper.Find(search);
    }

    [WebMethod]
    public bool EditBook(int bookId, Book newBook)
    {
        return bookDBHelper.EditBook(bookId, newBook);
    }

    [WebMethod]
    public bool DeleteBook(Book book)
    {
        return bookDBHelper.DeleteBook(book);
    }

    [WebMethod]
    public bool TakeBook(Book takenBook, Person reader)
    {
        return bookDBHelper.TakeBook(takenBook, reader);
    }

    [WebMethod]
    public bool AddNewBook(Book book)
    {
        return bookDBHelper.AddNewBook(book);
    }

    [WebMethod]
    public bool RenewBook(Book renewedBook)
    {
        return bookDBHelper.RenewBook(renewedBook);
    }

    [WebMethod]
    public bool IsBookAlreadyTaken(Book bookToCheck)
    {
        return bookDBHelper.IsBookAlreadyTaken(bookToCheck);
    }

    [WebMethod]
    public int GetNextId()
    {
        return bookDBHelper.GetNextId();
    }


}