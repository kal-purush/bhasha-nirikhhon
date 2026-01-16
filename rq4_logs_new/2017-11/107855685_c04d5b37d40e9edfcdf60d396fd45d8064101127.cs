using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lab4.ConsoleApp
{
    class Dreptunghi
    {
        public int lungimea;
        public int latimea;

        public Dreptunghi()
        {
            lungimea = 2;
            latimea = 345;
        }
        public static void ModificaValorile(int olungime, int olatime)
        {
            lungimea = olungime;
            latimea = olatime;
        }
        public static void AfiseazaValoarea()
        {
            Console.WriteLine("{0} este mai mica ca si {2}", lungimea, latimea);
            Console.ReadKey();
        }
    }
}