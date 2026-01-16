using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibraryManager
{
    struct Books
    {
        public int book_id;
        public string title;
        public string author;
        public string rl_date; //release date
        public int price;
        public byte quant_avail; //quantity available
    }

    internal class IBookHandler
    {
        static int n = 0;
        static Books[] books = new Books[100];

        public void BookLoad()
        {
            StreamReader sr = new StreamReader("books.txt");
            while (!sr.EndOfStream)
            {
                string[] data = Console.ReadLine().Split("|");
                books[n].book_id = int.Parse(data[0]);
                books[n].title = data[1];
                books[n].author = data[2];
                books[n].rl_date = data[3];
                books[n].price = int.Parse(data[4]);
                books[n].quant_avail = byte.Parse(data[5]);

                n++;
            }
        }

        public void AddEntry()
        {

        }
    }
}