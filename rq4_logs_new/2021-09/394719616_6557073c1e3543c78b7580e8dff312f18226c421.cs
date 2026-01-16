using System;

namespace OperacoesDateTime2
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime d = DateTime.Now;
            DateTime dd = new DateTime(2021, 09, 27, 23, 00, 00);

            DateTime d2 = d.AddHours(5);
            DateTime d3 = d.AddDays(7);

            Console.WriteLine(d);
            Console.WriteLine(d2);
            Console.WriteLine(d3);
            TimeSpan t = dd.Subtract(d); // para apresentar a diferença entre duas datas utilizamos a função 'Substract'
            Console.WriteLine(t);
        }
    }
}