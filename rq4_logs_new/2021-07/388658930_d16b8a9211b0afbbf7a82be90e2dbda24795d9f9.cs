using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookLibs
{
    public class Book
    {
        private String bookID;
        private String bookName;
        private String author;
        private String publisher;

        public Book(string bookID, string bookName, string author, string publisher)
        {
            this.BookID = bookID;
            this.BookName = bookName;
            this.Author = author;
            this.Publisher = publisher;
        }

        public string BookID { get => bookID; set => bookID = value; }
        public string BookName { get => bookName; set => bookName = value; }
        public string Author { get => author; set => author = value; }
        public string Publisher { get => publisher; set => publisher = value; }


        public void Input()
        {
            Console.WriteLine("Enter BookID: ");
            bookID = Console.ReadLine();
            Console.WriteLine("Enter BookName: ");
            bookName = Console.ReadLine();
            Console.WriteLine("Enter Author: ");
            author = Console.ReadLine();
            Console.WriteLine("Enter Publisher: ");
            publisher = Console.ReadLine();
        }

        public void Display()
        {
            Console.WriteLine("ID: {0}, Ten: {1}, Author: {2}, Publisher: {3} ", bookID, bookName, author, publisher);
        }
    }
}