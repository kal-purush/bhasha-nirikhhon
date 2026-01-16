using System;
using System.Collections.Generic;
using System.Text;

namespace AlexandriaMemorialLibrary
{
    class Book
    {
        public string Title { get; set; }
        public string Author { get; set; }
        public ulong ISBN { get; set; }
        public List<Enum> Genre = new List<Enum>();
        public List<Enum> Status = new List<Enum>();
        public DateTime DueDate { get; set; }

        public Book(string title, string author, ulong isbn, List<Enum>genre, List<Enum>status, DateTime dueDate)
        {
            this.Title = title;
            this.Author = author;
            this.ISBN = isbn;
            this.Genre = genre;
            this.Status = status;
            this.DueDate = dueDate;
        }


    }
}