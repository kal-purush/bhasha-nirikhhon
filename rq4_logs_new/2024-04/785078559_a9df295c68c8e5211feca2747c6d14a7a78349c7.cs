using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Generics
{
    class Example<T>
    {
        T box;
        public T Box 
        { 
            set { this.box = value; }
            get { return this.box; }
        }
        //public Example(T b) {this.box = b;}
        //public T getBox() { return this.box; }

    }
    internal class Program
    {
        static void Main(string[] args)
        {
            //create a object with specific data type
            Example<string> e = new Example<string>();
            e.Box = "vicky";
            Console.WriteLine(e.Box);

            Example<int> i = new Example<int>();
            i.Box = 15;
            Console.WriteLine(i.Box);

        }
    }
}