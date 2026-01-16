using System;

namespace OperacoesDateTime
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime d = new DateTime(2021, 09, 27, 13, 45, 58, 275);

            Console.WriteLine(d);
            Console.WriteLine("1) Data: " + d.Date);
            Console.WriteLine("2) Dia: " + d.Day);
            Console.WriteLine("3) Mês: " + d.Month);
            Console.WriteLine("4) Ano: " + d.Year);
            Console.WriteLine("5) Hora: " + d.Hour);
            Console.WriteLine("6) Minuto: " + d.Minute);
            Console.WriteLine("7) Segundos: " + d.Second);
            Console.WriteLine("8) Dia da Semana: " + d.DayOfWeek);
            Console.WriteLine("9) Dia do Ano: " + d.DayOfYear);
            Console.WriteLine("10) Tipo (local ou universal): " + d.Kind);
            Console.WriteLine("11) Mile Segundos: " + d.Millisecond);
            Console.WriteLine("12) Ticks: " + d.Ticks);
            Console.WriteLine("13) Hora do dia: " + d.TimeOfDay);
            Console.WriteLine("13) Data completa: " + d.ToLongDateString());
            Console.WriteLine("14) Hora completa: " + d.ToLongTimeString());
            Console.WriteLine("15) Data resumida: " + d.ToShortDateString());
            Console.WriteLine("16) Data resumida: " + d.ToShortTimeString());
            Console.WriteLine("17) Data hora resumida: " + d.ToString());
            Console.WriteLine("18) Data hora formatada manualmente: " + d.ToString("yyyy-MM-dd HH:mm:ss"));
            Console.WriteLine("19) Data hora formatada manualmente com milesegundos: " + d.ToString("yyyy-MM-dd HH:mm:ss.fff"));





        }
    }
}