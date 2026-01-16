using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CollectionsDemo
{
    class DictionaryDemo
    {
        public static void DictionaryOperations()
        {
            Dictionary<int, string> dictionary = new Dictionary<int,string>();
            Console.WriteLine("Enter the number of data to be added");
            int count = Convert.ToInt32(Console.ReadLine());
            for(int i = 0; i < count; i++)
            {
                Console.WriteLine("Enter the integer key");
                int key = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Enter the value of the key");
                string value = Console.ReadLine();
                dictionary.Add(key,value);
            }
            foreach(KeyValuePair<int,string> keyValue in dictionary)
            {
                Console.WriteLine("The key {0} = {1}",keyValue.Key,keyValue.Value);
            }


        }
    }
}