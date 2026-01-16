using System;
using System.Linq;
using Advent2017.Utilities;

namespace Advent2017.Solutions
{
    class Day16
    {
        public string Dancers = "abcdefghijklmnop";

        public void Dance(string input)
        {
            var instructions = input.Split(',');
            instructions.ForEach(x=>
            {
                if(x.StartsWith("s"))
                    Spin(Convert.ToInt32(x.Replace("s","")));
                if(x.StartsWith("x"))
                    Exchange(x);
                if(x.StartsWith("p"))
                    SwapPrograms(x);
            });
        }

        public void Spin(int amount)
        {
            var concat = string.Concat(new string(Dancers.Skip(Dancers.Length - amount).Take(amount).ToArray()),
                new string (Dancers.Take(Dancers.Length - amount).ToArray()));

            Dancers = concat;
        }

        public void Exchange(string exchange)
        {
            var parts = exchange.Replace("x", "")
                                .Split('/')
                                .Select(x => Convert.ToInt32(x))
                                .ToArray();

            Swap(parts[0],parts[1]);

        }

        public void SwapPrograms(string instruction)
        {
            var programs = instruction.Substring(1).Split('/');
            var first = Dancers.IndexOf(programs[0]);
            var second = Dancers.IndexOf(programs[1]);
            Swap(first,second);
        }

        private void Swap(int x, int y)
        {
            var array = Dancers.ToCharArray();
            var a = array[x];
            var b = array[y];

            array[x] = b;
            array[y] = a;

            Dancers = new string(array);
        }

        public void Part2(string steps)
        {
            Dance(steps);
            var count = 1;
            while (Dancers != "abcdefghijklmnop")
            {
                Dance(steps);
                count++;
            }
            var iterations = 1000000000 % count;

            Enumerable.Range(0, iterations).ForEach(x => Dance(steps));
        }
    }
}