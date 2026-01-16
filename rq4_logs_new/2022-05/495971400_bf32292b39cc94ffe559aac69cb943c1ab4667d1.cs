using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace part2
{
    class Necklace
    {
        public Stone[] necklace { get; set; }

        public void ShowNecklace()
        {
            foreach(var value in necklace)
            {
                Console.WriteLine($"Камінь: {value.Name} \nТип:{value.Type} \nЦіна:{value.Price} $  \nВага:{value.Weight} карат   \nПрозорість:{value.Vis} ");
                Console.WriteLine(new string('-', 40));
            }
        }

        public double CountWeight()
        {
            double count = 0;
            foreach (var item in necklace)
            {
                count += item.Weight * 10;
            }
            return count;
        }
        public double CountPrice()
        {
            double count = 0;
            foreach (var item in necklace)
            {
                count += item.Price * 10;
            }
            return count;
        }

        public void Sort()
        {
            var pricesort = necklace.OrderBy(x => x.Price);
            foreach (var x in pricesort)
                Console.WriteLine($"{x.Name} - {x.Price} $");
        }

        public void VisSort(int min, int max)
        {

            var res = necklace.Where(x => x.Vis >= min && x.Vis <= max);
            foreach(var x in res)
                Console.WriteLine($"Name: {x.Name}, visibility: {x.Vis}");
        }

    }
}