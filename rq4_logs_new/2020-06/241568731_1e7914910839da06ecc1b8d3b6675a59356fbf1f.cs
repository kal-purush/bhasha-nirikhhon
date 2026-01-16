using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SubjectArea
{
    public enum Genres
    {
        Drama,
        Melodrama,
        Comedy,
        Musical,
        Parody,
        Tragedy,
        NotFound
    }
    public sealed class Ticket
    {
        public double Price { get; private set; }
        public bool IsSold { get; private set; }
        public bool IsBooked { get; private set; }
        public Ticket(double price)
        {
            Price = price;
            IsSold = false;
            IsBooked = false;
        }
        public void Buy()
        {
            if (IsBooked)
            {
                return;
            }
            if (IsSold)
            {
                return;
            }
            IsSold = true;
        }
        public void Book()
        {
            if (IsBooked)
            {
                return;
            }
            if (IsSold)
            {
                return;
            }
            IsBooked = true;
        }
    }
    public sealed class Performance
    {
        public string Name { get; private set; }
        public string Author { get; private set; }
        public Genres Genre { get; private set; }
        public DateTime Date { get; private set; }
        Ticket[] tickets100 = new Ticket[1000];
        Ticket[] tickets300 = new Ticket[400];
        Ticket[] tickets500 = new Ticket[200];
        Ticket[] tickets1000 = new Ticket[80];
        int tickpointer100 = 0;
        int tickpointer300 = 0;
        int tickpointer500 = 0;
        int tickpointer1000 = 0;
        public Performance(string name, string author, Genres genre, DateTime date)
        {
            Name = name;
            Author = author;
            Genre = genre;
            Date = date;
            int c = 0;
            while (c < 1000)
            {
                if (c < 80) tickets1000[c] = new Ticket(1000);
                if (c < 200) tickets500[c] = new Ticket(500);
                if (c < 400) tickets300[c] = new Ticket(300);
                if (c < 1000) tickets100[c] = new Ticket(100);
                c++;
            }
        }
        public int TicketsLeft() => (1000 - tickpointer100) + (400 - tickpointer300) + (200 - tickpointer500) + (80 - tickpointer1000);
        public int TicketsLeft(int price)
        {
            switch (price)
            {
                case 100:
                    return 1000 - tickpointer100;
                case 300:
                    return 400 - tickpointer300;
                case 500:
                    return 200 - tickpointer500;
                case 1000:
                    return 80 - tickpointer1000;
                default:
                    return 0;
            }
        }
        public ref Ticket[] GetTickArr(int price)
        {
            switch (price)
            {
                case 100:
                    return ref tickets100;
                case 300:
                    return ref tickets300;
                case 500:
                    return ref tickets500;
                case 1000:
                    return ref tickets1000;
                default:
                    throw new Exception("Invalid price");
            }
        }
        public ref int GetTickPtr(int price)
        {
            switch (price)
            {
                case 100:
                    return ref tickpointer100;
                case 300:
                    return ref tickpointer300;
                case 500:
                    return ref tickpointer500;
                case 1000:
                    return ref tickpointer1000;
                default:
                    throw new Exception("Invalid price");
            }
        }
        public override string ToString() => $"Название: {Name}. Автор: {Author}. Жанр: {Affiche.TranslateGenre(Genre.ToString())}.";
    }
    public sealed class Affiche
    {
        public List<Performance> Performances { get; private set; }
        public List<string> Authors = new List<string>();
        public List<string> Names = new List<string>();
        public List<string> Genres = new List<string>();
        public DateTime Date { get; set; }
        public Affiche(Performance[] performances)
        {
            foreach (Performance perf in performances)
            {
                Performances.Add(perf);
                if (!Authors.Contains(perf.Author)) Authors.Add(perf.Author);
                if (!Names.Contains(perf.Name)) Names.Add(perf.Name);
                if (!Genres.Contains(TranslateGenre(perf.Genre.ToString()))) Genres.Add(TranslateGenre(perf.Genre.ToString()));
            }
            Date = DateTime.Now;
        }
        public Affiche(List<Performance> performances)
        {
            Performances = performances;
            foreach (Performance perf in Performances)
            {
                if (!Authors.Contains(perf.Author)) Authors.Add(perf.Author);
                if (!Names.Contains(perf.Name)) Names.Add(perf.Name);
                if (!Genres.Contains(TranslateGenre(perf.Genre.ToString()))) Genres.Add(TranslateGenre(perf.Genre.ToString()));
            }
            Date = DateTime.Now;
        }
        public List<Performance> GetByAuthor(string author)
        {
            List<Performance> temp = new List<Performance>();
            foreach (Performance perf in Performances) if (perf.Author == author) temp.Add(perf);
            return temp;
        }
        public List<Performance> GetByName(string name)
        {
            List<Performance> temp = new List<Performance>();
            foreach (Performance perf in Performances) if (perf.Name == name) temp.Add(perf);
            return temp;
        }
        public List<Performance> GetByGenre(string genre)
        {
            List<Performance> temp = new List<Performance>();
            foreach (Performance perf in Performances) if (Affiche.TranslateGenre(perf.Genre.ToString()) == genre) temp.Add(perf);
            return temp;
        }
        public static string TranslateGenre(string genre)
        {
            if (genre == "Comedy") return "Комедия";
            else if (genre == "Drama") return "Драма";
            else if (genre == "Melodrama") return "Мелодрама";
            else if (genre == "Musical") return "Мюзикл";
            else if (genre == "Parody") return "Пародия";
            else if (genre == "Tragedy") return "Трагедия";
            else return "Неизвестно";
        }
    }
}