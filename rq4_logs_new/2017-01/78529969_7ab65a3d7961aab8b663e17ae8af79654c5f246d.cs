using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TDD_TEST
{
    public class MyCalculator
    {
        public List<int> costAdd()
        {
            var items = new List<Product>
            {
                new Product { Id=1, Cost=1,Revenue=11,SellPrice=21},
                new Product { Id=2, Cost=2,Revenue=12,SellPrice=22},
                new Product { Id=3, Cost=3,Revenue=13,SellPrice=23},
                new Product { Id=4, Cost=4,Revenue=14,SellPrice=24},
                new Product { Id=5, Cost=5,Revenue=15,SellPrice=25},
                new Product { Id=6, Cost=6,Revenue=16,SellPrice=26},
                new Product { Id=7, Cost=7,Revenue=17,SellPrice=27},
                new Product { Id=8, Cost=8,Revenue=18,SellPrice=28},
                new Product { Id=9, Cost=9,Revenue=19,SellPrice=29},
                new Product { Id=10, Cost=10,Revenue=20,SellPrice=30},
                new Product { Id=11, Cost=11,Revenue=21,SellPrice=31},
            };

            List<int> output = new List<int> { };

            int sum = 0;
            int group = 0;

            for (int i = 0; i < items.Count; i++)
            {
                if (group == 2 || i == items.Count - 1)
                {
                    sum = sum + items[i].Cost;
                    output.Add(sum);
                    group = 0;
                    sum = 0;
                }
                else
                {
                    sum = sum + items[i].Cost;
                    group++;
                }
            }

            return output;
        }

        public List<int> revenueAdd()
        {
            var items = new List<Product>
            {
                new Product { Id=1, Cost=1,Revenue=11,SellPrice=21},
                new Product { Id=2, Cost=2,Revenue=12,SellPrice=22},
                new Product { Id=3, Cost=3,Revenue=13,SellPrice=23},
                new Product { Id=4, Cost=4,Revenue=14,SellPrice=24},
                new Product { Id=5, Cost=5,Revenue=15,SellPrice=25},
                new Product { Id=6, Cost=6,Revenue=16,SellPrice=26},
                new Product { Id=7, Cost=7,Revenue=17,SellPrice=27},
                new Product { Id=8, Cost=8,Revenue=18,SellPrice=28},
                new Product { Id=9, Cost=9,Revenue=19,SellPrice=29},
                new Product { Id=10, Cost=10,Revenue=20,SellPrice=30},
                new Product { Id=11, Cost=11,Revenue=21,SellPrice=31},
            };

            List<int> output = new List<int> { };

            int sum = 0;
            int group = 0;

            for (int i = 0; i < items.Count; i++)
            {
                if (group == 3 || i == items.Count - 1)
                {
                    sum = sum + items[i].Revenue;
                    output.Add(sum);
                    group = 0;
                    sum = 0;
                }
                else
                {
                    sum = sum + items[i].Revenue;
                    group++;
                }
            }

            return output;
        }
    }

    internal class Product
    {
        public int Id { get; set; }

        public int Cost { get; set; }

        public int Revenue { get; set; }

        public int SellPrice { get; set; }
    }
}