using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            int hours, minutes, hours12;
            string daypart;
            DateTime time = DateTime.Now;
            hours = time.Hour;
            minutes = time.Minute;

            if (hours >= 12)
            {
                hours12 = hours - 12;
                daypart = "PM";
            }
            else {
                hours12 = hours;
                daypart = "AM";
            }
            Console.WriteLine(hours12 +" "+ daypart + " " + minutes + " minuten");
            Console.ReadLine();
            

        }
    }
}