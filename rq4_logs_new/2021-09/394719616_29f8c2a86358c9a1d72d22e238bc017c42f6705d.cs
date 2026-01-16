using System;

namespace FuncaoTimeSpan
{
    class Program
    {
        static void Main(string[] args)
        {
            TimeSpan t1 = new TimeSpan(0, 1, 30); // apresenta um intervalo de tempo no formato estabelecido nos parâmtros
            TimeSpan t2 = new TimeSpan(900000000L); // apresenta um intervado de tempo no formato de nano segundos
            TimeSpan t3 = new TimeSpan(2, 11, 21); // apresentar um intervado de tempo no formato informado nos parâmetros
            TimeSpan t4 = new TimeSpan(1, 2, 11, 21); // apresentar um intervado de tempo no formato informado nos parâmetros
            TimeSpan t5 = new TimeSpan(1, 2, 11, 21, 321); // apresentar um intervado de tempo no formato informado nos parâmetros
            TimeSpan t6 = TimeSpan.FromDays(1.5); // apresenta o intervalo de tempo na unidade em que você deseja (neste caso em dias). Aqui está sendo apresentado 1.5/dia ou seja um dia e 12 horas
            TimeSpan t7 = TimeSpan.FromHours(1.5); // apresenta o intervalo de tempo na unidade em que você deseja (neste caso em horas). Aqui está sendo apresentado 1.5/horas ou seja uma hora e meia
            TimeSpan t8 = TimeSpan.FromMinutes(1.5); // apresenta o intervalo de tempo na unidade em que você deseja (neste caso em minutos). Aqui está sendo apresentado 1.5/minutos ou seja um minuto e meio
            TimeSpan t9 = TimeSpan.FromSeconds(1.5);
            TimeSpan t10 = TimeSpan.FromMilliseconds(1.5);
            TimeSpan t11 = TimeSpan.FromTicks(900000000L);

            Console.WriteLine(t1);
            Console.WriteLine(t1.Ticks); // apresenta o objeto 't1' em formato de nano segundos
            Console.WriteLine(t2);
            Console.WriteLine(t3);
            Console.WriteLine(t4);
            Console.WriteLine(t5);
            Console.WriteLine(t6);
            Console.WriteLine(t7);
            Console.WriteLine(t8);
            Console.WriteLine(t9);
            Console.WriteLine(t10);
            Console.WriteLine(t11);
        }
    }
}