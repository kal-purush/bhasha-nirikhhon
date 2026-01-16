using System;

namespace PadraoDateTimeKind
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime d1 = new DateTime(2021, 9, 28, 20, 10, 30, DateTimeKind.Local);
            DateTime d2 = new DateTime(2021, 9, 28, 20, 10, 30, DateTimeKind.Utc);
            DateTime d3 = new DateTime(2021, 9, 28, 20, 10, 30);

            Console.WriteLine("d1: " + d1);
            Console.WriteLine("d1 Kind: " + d1.Kind);
            Console.WriteLine("d1 to Local: " + d1.ToLocalTime());
            Console.WriteLine("d1 to Utc: " + d1.ToUniversalTime());
            Console.WriteLine();

            Console.WriteLine("d2: " + d2);
            Console.WriteLine("d2 Kind: " + d2.Kind);
            Console.WriteLine("d2 to Local: " + d2.ToLocalTime());
            Console.WriteLine("d2 to Utc: " + d2.ToUniversalTime());
            Console.WriteLine();

            Console.WriteLine("PADRÃO ISO");
            DateTime d4 = DateTime.Parse("2021-09-28 15:30:55");
            DateTime d5 = DateTime.Parse("2021-09-28T15:30:55Z"); // esse padrão armazena no formato UTC e instancia/exibe no formato LOCAL
            Console.WriteLine(d4);
            Console.WriteLine(d5);
            Console.WriteLine();

            Console.WriteLine("d4: " + d4);
            Console.WriteLine("d4 Kind: " + d4.Kind);
            Console.WriteLine("d4 to Local: " + d4.ToLocalTime());
            Console.WriteLine("d4 to Utc: " + d4.ToUniversalTime());
            Console.WriteLine();
            Console.WriteLine("d5: " + d5);
            Console.WriteLine("d5 Kind: " + d5.Kind);
            Console.WriteLine("d5 to Local: " + d5.ToLocalTime());
            Console.WriteLine("d5 to Utc: " + d5.ToUniversalTime());
            Console.WriteLine();
            Console.WriteLine(d5.ToString("yyyy-MM-ddTHH:mm:ssZ")); // cuidado, não foi especificado antes o padrão universal!
            Console.WriteLine(d5.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")); // este está correto, pois foi especificado o padrão universal

        }
    }
}