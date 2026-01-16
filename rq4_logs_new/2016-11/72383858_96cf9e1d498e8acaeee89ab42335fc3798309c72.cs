using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeferedExecultion
{
    public class Movie
    {
        public string Title { get; set; }
        int _year;
        public int Year
        {
            get
            {
                //Console.WriteLine($"render {_year} for {Title}");
                return _year;
            }
            set { _year = value; }
        }

        float _rate;
        public float Rating
        {
            get {
               // Console.WriteLine($"render year {Year} of this {Title}");
                return _rate; }
            set { _rate = value; }
        }

    }
}