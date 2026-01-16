using System;

namespace PropriedadesTimeSpan
{
    class Program
    {
        static void Main(string[] args)
        {
            TimeSpan t = new TimeSpan(1, 30, 10);
            TimeSpan t2 = new TimeSpan(0, 10, 5);

            // PROPRIEDADES DE TIMESPAN

            // Retorna valores inteiros
            //Console.WriteLine(t);
            //Console.WriteLine("Dias: " + t.Days);
            //Console.WriteLine("Horas: " + t.Hours);
            //Console.WriteLine("Minutos: " + t.Minutes);
            //Console.WriteLine("Segundos: " + t.Seconds);
            //Console.WriteLine("Milesegundos: " + t.Milliseconds);
            //Console.WriteLine("Ticks: " + t.Ticks);

            // Retorna valores quebrados (fração)
            //Console.WriteLine("Total dias: " + t.TotalDays);
            //Console.WriteLine("Total horas: " + t.TotalHours);
            //Console.WriteLine("Total minutos: " + t.TotalMinutes);
            //Console.WriteLine("Total segundos: " + t.TotalSeconds);
            //Console.WriteLine("Total Milesegundos:  " + t.TotalMilliseconds);

            // OPERAÇÕES COM TIMESPAN
            TimeSpan soma = t.Add(t2); // soma de tempo (timespan)
            TimeSpan dif = t.Subtract(t2); // subtração de tempo (timespan)
            TimeSpan mult = t2.Multiply(2.0); // multiplicação de tempo (timespan)
            TimeSpan div = t2.Divide(2.0); // divisão de tempo (timespan)

            Console.WriteLine("t: " + t);
            Console.WriteLine("t2: " + t2);
            Console.WriteLine();
            Console.WriteLine("Soma de: t + t2: " + soma);
            Console.WriteLine("Subtração de: t - t2: " + dif);
            Console.WriteLine("Multiplicação de t2: " + mult);
            Console.WriteLine("Divisão de t2: " + div);
        }
    }
}