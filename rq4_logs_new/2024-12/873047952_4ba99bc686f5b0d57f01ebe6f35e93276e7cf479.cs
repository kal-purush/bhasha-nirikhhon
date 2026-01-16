using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1.Classes
{
    public class Book
    {
        public string Title { get; private set; }
        public string Author { get; private set; }
        public int Year { get; private set; }
        public Book(string title, string author, int year) {
            this.Title = title;
            this.Author = author;
            this.Year = year;
        }

        public override string ToString()
        {
            return Author + " - \"" + Title + "\", wydana w " + Year.ToString() + "r.";
        }
    }
}