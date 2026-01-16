using Support;
using System;

namespace Generator
{
    public class Program
    {
        public void Main(string[] args)
        {
            Console.WriteLine(Isbn10API.Generate());
        }
    }
}