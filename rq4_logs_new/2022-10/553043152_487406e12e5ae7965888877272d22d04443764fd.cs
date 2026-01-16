using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text.RegularExpressions;

namespace Lab7
{
    public class ObjectWithName
    {
        public ObjectWithName(string name)
        {
            Name = name;
        }

        public string Name { get; set; }
    }


    public static class LinqQueries
    {
        public static string Query1(IEnumerable<ObjectWithName> list, Char delimeter)
        {

            var query = list.Skip(3).Select(i => i.Name).Aggregate((i, j) => i + delimeter + j);
            return query;
        }
        public static IEnumerable<ObjectWithName> Query2(IEnumerable<ObjectWithName> list)
        {

            var query = list.Where((elem, index) => elem.Name.Length > index);
            return query;
        }
        public static void Query3(string phrase)
        {
            Regex rgx = new Regex("[^a-zA-Z�-��-� ]");
            Regex rgx1 = new Regex(" +");
            phrase = rgx.Replace(phrase, "");
            phrase = rgx1.Replace(phrase, " ");

            var words = phrase.Split(" ");
            Console.WriteLine(phrase);

            var query = words.GroupBy(w => w.Length).OrderByDescending(n => n.Count()).ToList();
            Console.WriteLine(query);
            foreach (var group in query)
            {
                Console.WriteLine("{0}, {1}", group.Key, group.Count());
                foreach(var word in group)
                {
                    Console.WriteLine(word);
                }
                Console.WriteLine("");
            }
        }
    }
}