using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cars
{
    class Program
    {
        static void Main(string[] args)
        {
            var cars = ProcessFile("fuel.csv");

            foreach (var item in cars)
            {
                Console.WriteLine(item.Name);
            }
        }

        private static List<car> ProcessFile(string path)
        {
            return
            File.ReadAllLines(path)
                .Skip(1) //pagination
                .Where(L => L.Length > 1)
                .Select(car.ParseFromCSV)
                .ToList();
        }
    }
}