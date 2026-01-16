using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Block3LabLibrary;

namespace Block3Lab
{
    internal class ComplexDataObjectsLab
    {
        static void Main(string[] args)
        {
            Product p1 = new Product(123, "Cleaner", 4455, "Counter Top Disinfectant", 3.49m);
            Console.WriteLine(p1);
            Product p2 = new Product(230, "Toothpaste", 6454, "Minty Fresh", 5.29m);
            Product p3 = new Product(111, "Broccoli", 1134, "Fresh Broccoli", 3.98m);
            Product p4 = new Product(222, "Dog Toy", 1445, "Plush & Squeaky", 1.99m);
            Product p5 = new Product(321, "Road Bike", 5873, "Carbon Fiber", 3500m);
            Product p6 = new Product(908, "Binder", 52345, "Hardback Binder", 5.49m);

            List<Product> products = new List<Product>();
            products.Add(p1);
            products.Add(p2);
            products.Add(p3);
            products.Add(p4);
            products.Add(p5);
            products.Add(p6);

            foreach (var product in products)
            {
                Console.WriteLine("\n\n-=-=-=-=-=-=-=\n");
                Console.WriteLine(product);
            }


            Console.WriteLine("\n\nDictonary Entry\n\n");

            Dictionary<string, Product> pd2 = new Dictionary<string, Product>()
            {
                {p1.Name, p1 },
                {p2.Name, p2 },
                {p3.Name, p3 },
                {p4.Name, p4 },
                {p5.Name, p5 },
                {p6.Name, p6 },

            };

            foreach (KeyValuePair<string, Product> kvp in pd2)
            {
                Console.WriteLine(kvp.Value);
            }



        }//end Main()
    }//end Class
}//end Namespace