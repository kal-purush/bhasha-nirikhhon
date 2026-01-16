using System;
using System.Globalization;

namespace FuncaoData
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime d1 = new DateTime(2021, 09, 27); // apresenta a data do parâmetro '27/09/2021 00:00:00'
            DateTime d2 = new DateTime(2021, 09, 27, 20, 45, 3); // apresenta a data/hora do parâmetro '27/09/2021 20:45:03'
            DateTime d3 = new DateTime(2021, 09, 27, 20, 45, 3, 500); // apresenta a data/hora do parâmetro com milesegundos '27/09/2021 20:45:03' 
            DateTime d4 = DateTime.Now; // apresenta a data/hora do sistema
            DateTime d5 = DateTime.Today; // apresenta a data do dia atual às zero horas (00:00:00)
            DateTime d6 = DateTime.UtcNow; // apresenta a data/hora atual em Greenwich
            DateTime d7 = DateTime.Parse("2021-09-27"); // converte a string apontada no parâmetro para o formato data/hora
            DateTime d8 = DateTime.Parse("2021-09-27 15:30:15"); // converte a string apontada no parâmetro para o formato data/hora
            DateTime d9 = DateTime.Parse("27/09/2021"); // converte a string apontada no parâmetro para o formato data/hora do Brasil
            DateTime d10 = DateTime.Parse("27/09/2021 16:45:10"); // converte a string apontada no parâmetro para o formato data/hora do Brasil
            DateTime d11 = DateTime.ParseExact("2021-09-27", "yyyy-MM-dd", CultureInfo.InvariantCulture); // para determinar o formato de data que quero utilizar
            DateTime d12 = DateTime.ParseExact("27/09/2021 13:05:55", "dd/MM/yyyy HH:mm:ss", CultureInfo.InvariantCulture); // para determinar o formato de data/hora que quero utilizar

            Console.WriteLine(d1);
            Console.WriteLine(d2);
            Console.WriteLine(d3);
            Console.WriteLine(d4);
            Console.WriteLine(d5);
            Console.WriteLine(d6);
            Console.WriteLine(d7);
            Console.WriteLine(d8);
            Console.WriteLine(d9);
            Console.WriteLine(d10);
            Console.WriteLine(d11);
            Console.WriteLine(d12);
        }
    }
}