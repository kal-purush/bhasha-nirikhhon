using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Durchschnittsrechner
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Durchschnittsrechner V1.2";
            
            // Code
            int count;

            int countH;

            double result;

            List<string> Noten = new List<string>();

            List<string> Hauptfachnoten = new List<string>();

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine("Notendurchschnittsrechner by Rafael Wickberg // GLifeDE");

            Console.ForegroundColor = ConsoleColor.White;

            Console.Write("Wie viele Hauptfächer gibt es?: ");
            countH = Convert.ToInt32(Console.ReadLine());

            Console.Write("Wie viele Nebenfächer gibt es?: ");
            count = Convert.ToInt32(Console.ReadLine());

            Console.ForegroundColor = ConsoleColor.Cyan;
            
            for (int i = 0; i < countH; i++)
            {
                Console.Write("Gebe eine Hauptfachnote ein: ");
                Hauptfachnoten.Add(Console.ReadLine());
            }

            Console.ForegroundColor = ConsoleColor.DarkCyan;

            for (int i = 0; i < count; i++)
            {
                Console.Write("Gebe eine Nebenfachnote ein: ");
                Noten.Add(Console.ReadLine());
            }

            double total = Noten.Sum(x => Convert.ToDouble(x));

            double totalH = Hauptfachnoten.Sum(x => Convert.ToDouble(x));

            

            result = (((totalH * 2) + total) / (count+(countH*2)));

            Console.ForegroundColor = ConsoleColor.Red;

            Console.WriteLine("Der Durchschnitt beträgt " + Math.Round(result, 2));

            if (result >= 3)
            {
                Console.WriteLine("Nächstes Mal wird's besser!");
            }
            else
            {
                Console.WriteLine("Sehr gut, weiter so!");
            }
         

            //Warte bevor schließen
            Console.ReadKey();

        }
    }
}