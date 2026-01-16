using System;
using System.Collections.Generic;
using System.Text;

namespace AlexandriaMemorialLibrary
{
    class BookListView
    {
        private List<Book> Library = new List<Book>();

        public BookListView(List<Book> library)
        {
            this.Library = library;
        }

        public void Display()
        {
            int i = 1;

            foreach (Book currentBook in Library)
            {
                string listing = i + ": " + currentBook.Title;
                if (currentBook.Status == Status.CheckedOut)
                {
                    if (i <10)
                    {
                        listing.Insert(2, "<CHECKED OUT>");
                    }
                    else if (i < 100)
                    {
                        listing.Insert(3, "<CHECKED OUT>");
                    }
                    else if (i < 1000)
                    {
                        listing.Insert(4, "<CHECKED OUT>");
                    }

                }
                Console.WriteLine(listing);
            }
        }
    }
}