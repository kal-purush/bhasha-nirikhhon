namespace exercise2
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Kuinka paljon maatilat");
            int maatilatAmount = Int32.Parse(Console.ReadLine());
            Console.WriteLine("Kuinka paljon herttuakunnat");
            int herttuakunnatAmount = Int32.Parse(Console.ReadLine());
            Console.WriteLine("Kuinka paljon maakunnat");
            int maakunnatAmount = Int32.Parse(Console.ReadLine());

            int maatilat = 1;
            int herttuakunnat = 3;
            int maakunnat = 6;

            int pelaaja = (maakunnatAmount * maatilat) 
            + (herttuakunnatAmount * herttuakunnat) 
            + (maakunnatAmount * maakunnat);

            Console.WriteLine(pelaaja);
        }
    }

}