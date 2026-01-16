using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exercise_five
{
    class Program
    {
        static void Main(string[] args)
        {
            string input;

            Console.WriteLine("Arva ära ühissõidukid");

            Console.WriteLine("Sisesta sõna: ");

            string[] transport = new string[] {"buss", "rong", "tramm", "laev", "lennuk", "troll", "metroo", "monorail", "marsruuttakso" };

            Random rnd = new Random();

            int rndIndex = rnd.Next(0, transport.Length);

            string rndTransport = transport[rndIndex];

            StringBuilder sb = new StringBuilder();

            string[] words = rndTransport.Split(' ');

            for(int i = 0; i <= words.Length; i++)
            {
                Console.WriteLine(words[i]);
            }


           // Console.Write("Sisesta täht: ");

            //input = Console.ReadLine();



            Console.WriteLine(rndTransport);

            Console.ReadLine();
        }
    }
}