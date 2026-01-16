using System;
using System.Collections.Generic;

namespace SpacedOutLibrary
{
    public class Controller
    {
        List<Book> Books = new List<Book>();

        List<string> book = new List<string>() {"Farenheit 451", "Ray Radbury", "Dune",
                "Frank Herbert", "The Callista Chronicles", "Callista Gloss", "Frankenstein", "Mary Shelly",
            "A Wrinkle in Time", "Madeleine L'Engle", "2001", "Arthur C. Clarke",
                "Ender's Game", "Orson Scott Card", "Star Wars", "George Lucas",
            "I,Robot","Issac Asimov", "Jurassic Park", "Michael Crichton", "1984", "George Orwell", "Avengers End Game", "Stan Lee",
            };

        public Controller()
        {
            for (int i = 0; i < book.Count; i += 2)
            {
                Book b = new Book();

                b.Title = book[i];

                b.Author = book[i + 1];

                b.Status = false;
                b.DueDate = "";

                Books.Add(b);
            }

            Books[0].DueDate = "15/05/2019";
            Books[0].Status = true;



        }

        public void Run()
        {
            BookListView blv = new BookListView(Books);
            blv.Display();

        }
    }
}