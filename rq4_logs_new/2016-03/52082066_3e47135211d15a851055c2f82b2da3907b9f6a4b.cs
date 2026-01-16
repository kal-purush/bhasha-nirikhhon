using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HashTable
{

    public class Hash
    {
        private static int N = 1000;
        private List[] hash = new List[N];

        /// <summary>
        /// constructor for hashtable
        /// </summary>
        public Hash()
        {
            for (int i = 0; i < N; ++i)
            {
                hash[i] = new List();
            }
        }

        /// <summary>
        /// make hash function
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public int hashFunction(string element)
        {
            int symb = 0;
            for (int i = 0; i < element.Length; i++)
            {
                symb += element[i];

            }
            return symb % N;
        }

        /// <summary>
        /// add element in hashtable
        /// </summary>
        /// <param name="element"></param>
        public void Add(string element)
        {
            hash[hashFunction(element)].Push(element);
        }

        /// <summary>
        /// delete element from hashtable
        /// </summary>
        /// <param name="element"></param>
        public void Delete(string element)
        {
            if (hash[hashFunction(element)].Pop(element) != "")
            {
                Console.WriteLine("Delete");
            }
            else Console.WriteLine("Not found");
        }

        /// <summary>
        /// search element in hashtable
        /// </summary>
        /// <param name="element"></param>
        public void Search(string element)
        {
            if (hash[hashFunction(element)].Find(element))
            {
                Console.WriteLine("Found");
            }
            else Console.WriteLine("Not found");
        }

        /// <summary>
        /// priant all elements
        /// </summary>
        public void Print()
        {
            for (int i = 0; i < N; ++i)
            {
                if (hash[i].HaveElements())
                {
                    hash[i].Print();
                    Console.WriteLine();
                }
            }
        }
    }

}