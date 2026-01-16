using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace ConsoleApp0925_3
{
    class DateTIme2
    {
        static void Main()
        {

            System.Timers.Timer timer1 = new System.Timers.Timer();
            timer1.Interval = 1000;     //밀리세컨드
            timer1.Elapsed += Timer1_Elapsed;
            timer1.Start();

            Console.ReadLine();
        }

        private static void Timer1_Elapsed(object sender, ElapsedEventArgs e)
        {
            Console.WriteLine(DateTime.Now);
        }


        

    }
}