using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ComposeWord
{
    class Program
    {
        static void Main(string[] args)
        {
            string[] rules = { "O>M", "M>E", "R>O" };
            Console.WriteLine("What word stand over this rules?");
            foreach (var rule in rules)
            {
                Console.Write(rule + " ");
            }
            Console.WriteLine();
            Console.WriteLine(WordComposing(rules));
            Console.ReadKey();
        }

        static string WordComposing(string[] rules)
        {
            LinkedList<char> word = new LinkedList<char>();
            LinkedListNode<char> node;
            foreach(var rule in rules)
            {
                if((node = word.Find(rule[0])) != null)
                {
                    word.AddAfter(node, rule[2]);
                }
                else if((node = word.Find(rule[2])) != null)
                {
                    word.AddBefore(node, rule[0]);
                }
                else
                {
                    word.AddLast(rule[0]);
                    word.AddLast(rule[2]);
                }
            }
            return string.Join("", word.Select(c => c));
        }
    }
}