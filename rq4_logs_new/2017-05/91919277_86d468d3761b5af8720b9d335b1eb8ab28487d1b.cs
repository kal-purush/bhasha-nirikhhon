using System;
using System.Collections.Generic;

namespace Shared
{
    [Serializable]
    public class Book
    {
        int id;
        string title;
        string author;
        string publisher;
        string pubDate;
        string genre;
        int volume;
        int edition;

        public Book(int id, string title, string author, string publisher, string pubDate,
                  string genre, int volume, int edition)
        {
            this.id = id;
            this.title = title;
            this.author = author;
            this.publisher = publisher;
            this.pubDate = pubDate;
            this.genre = genre;
            this.volume = volume;
            this.edition = edition;
        }

        public int Id
        {
            get { return id; }
            set { id = value; }
        }

        public string Title
        {
            get { return title; }
            set { title = value; }
        }

        public string Author
        {
            get { return author; }
            set { author = value; }
        }

        public string Publisher
        {
            get { return publisher; }
            set { publisher = value; }
        }

        public string PubDate
        {
            get { return pubDate; }
            set { pubDate = value; }
        }

        public string Genre
        {
            get { return genre; }
            set { genre = value; }
        }

        public int Volume
        {
            get { return volume; }
            set { volume = value; }
        }

        public int Edition
        {
            get { return edition; }
            set { edition = value; }
        }
    }

    class BookList
    {
        List<Book> book;

        public BookList()
        {
            book = new List<Book>();
        }

        public void addSomeExamples()
        {
            book.Add(new Book(0, "Dom Quixote", "Miguel de Cervantes",
                                "Francisco de Robles", "1605", "Novel", 1, 1));

            book.Add(new Book(1, "Ulysses", "James Joyces",
                                "Sylvia Beach", "1922", "Novel", 1, 1));

            book.Add(new Book(2, "War and Peace", "Leo Tolstoy", 
                                "The Russian Messenger", "1869", "Novel", 1, 1));

            book.Add(new Book(3, "The Great Gatsby", "F. Scott Fitzgerald",
                                "Charles Scribner's Sons", "1925", "Novel", 1, 1));

            book.Add(new Book(4, "Pride and Prejudice", "Jane Austen",
                                "T Egerton, Whitehall", "1813", "Novel", 1, 1));

            book.Add(new Book(5, "The Hunger Games, #1", "Suzanne Collins",
                                "Scholastic Press", "2008", "Adventure", 1, 1));

            book.Add(new Book(6, "Harry Potter, #1", "J.K. Rowling",
                                "Arthur A. Levine Books", "1997", "Fantasy", 1, 1));

            book.Add(new Book(7, "Twilight", "Stephenie Meyer",
                                "Litter, Brown and Company", "2005", "Fantasy", 1, 1));

            book.Add(new Book(8, "The Hobbit", "J.R.R. Tolkien",
                                "George Allen & Unwin", "1937", "Fantasy", 1, 1));

            book.Add(new Book(9, "Divergent", "Veronica Roth",
                                "Katherine Tegen Books", "2011", "Science fiction", 1, 1));
        }

        public void addBook(int id, string title, string author, string publisher,
                            string pubDate, string genre, int volume, int edition)
        {
            book.Add(new Book(id, title, author, publisher, pubDate, genre, volume, edition));
        }

        public bool removeBook(int id)
        {
            foreach (Book bk in book)
            {
                if (bk.Id.Equals(id))
                {
                    book.Remove(bk);
                    return true;
                }
            }
            return false;
        }

        public Book getBook(int id)
        {
            foreach (Book bk in book)
            {
                if (bk.Id.Equals(id))
                    return bk;
            }
            return null;
        }

        public Book getBook(string title)
        {
            foreach (Book bk in book)
            {
                if (bk.Title.Equals(title))
                    return bk;
            }
            return null;
        }

        public List<Book> getAllByAuthor(string author)
        {
            List<Book> booksByAuthor = new List<Book>();

            foreach (Book bk in book)
            {
                if (bk.Author.Equals(author))
                    booksByAuthor.Add(bk);
            }

            return booksByAuthor;
        }

        public List<Book> getAllByGenre(string genre)
        {
            List<Book> booksByGenre = new List<Book>();

            foreach (Book bk in book)
            {
                if (bk.Genre.Equals(genre))
                    booksByGenre.Add(bk);
            }

            return booksByGenre;
        }

        public List<Book> getAllByPublisher(string publisher)
        {
            List<Book> booksByPublisher = new List<Book>();

            foreach (Book bk in book)
            {
                if (bk.Publisher.Equals(publisher))
                    booksByPublisher.Add(bk);
            }

            return booksByPublisher;
        }
    }
}