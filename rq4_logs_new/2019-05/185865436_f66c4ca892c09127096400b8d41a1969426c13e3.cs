using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpacedOutLibrary
{
    class BookListView
    {
        //prop
        public List<Book> DisplayBooks {get; set;}
        
        //const
        public BookListView (List<Book> DisplayBooks)
        {
            this.DisplayBooks = DisplayBooks;
        }

        //meth
        public void Display()
        {
            Console.Clear();
            Console.WriteLine("Welcome to the Spaced Out Library");
            Console.WriteLine("We offer a unique selection of sci-fi literature");
            Console.WriteLine();
            Console.WriteLine("Title:               Author:              Due Date:");
            for(int i = 0; i < DisplayBooks.Count; i++)
            {
                Console.WriteLine($"{DisplayBooks[i].Title,-20} {DisplayBooks[i].Author,-20} {(DisplayBooks[i].Status == true ? "Available" : DisplayBooks[i].DueDate.ToString())}");
            }
        }

    }
}