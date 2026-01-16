using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibraryManager
{
    internal class Book
    {
        private int book_id;
        private string title;
        private string author;
        private string rl_date; //release date
        private int price;
        private byte quant_avail; //quantity available

        public int Book_id { get => book_id; set => book_id = value; }
        public string Title { get => title; set => title = value; }
        public string Author { get => author; set => author = value; }
        public string Rl_date { get => rl_date; set => rl_date = value; }
        public int Price { get => price; set => price = value; }
        public byte Quant_avail { get => quant_avail; set => quant_avail = value; }
    
        public Book(int book_id, string title, string author, string rl_date, int price, byte quant_avail)
        {
            Book_id = book_id;
            Title = title;
            Author = author;
            Rl_date = rl_date;
            Price = price;
            Quant_avail = quant_avail;
        }
    }
}