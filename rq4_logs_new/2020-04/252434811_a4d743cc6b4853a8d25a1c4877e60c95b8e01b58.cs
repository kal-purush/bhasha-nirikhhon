using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    public static class DZ7
    {
        public static void Start()
        {
            Dictionary<string, int> people = new Dictionary<string, int>();
            people.Add("Chewbacca", 1);
            people.Add("Luk", 2);
            foreach (var pair in people)
            {
                Console.WriteLine($"Key = {pair.Key}, Value = {pair.Value}");
            }
            if (people.ContainsKey("Luk1"))
            {
                Console.WriteLine(people["Luk1"]);
            }
            
            Console.ReadLine();
        }
    }
}