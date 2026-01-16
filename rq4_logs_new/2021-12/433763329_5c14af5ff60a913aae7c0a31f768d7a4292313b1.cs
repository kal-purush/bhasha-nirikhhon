using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    internal class Day8
    {

        public static int Run()
        {
            Dictionary<string, int> possibleValues = new Dictionary<string, int>();
            possibleValues.Add("123567", 0);
            possibleValues.Add("36", 1);
            possibleValues.Add("13457", 2);
            possibleValues.Add("13467", 3);
            possibleValues.Add("2346", 4);
            possibleValues.Add("12467", 5);
            possibleValues.Add("124567", 6);
            possibleValues.Add("136", 7);
            possibleValues.Add("1234567", 8);
            possibleValues.Add("123467", 9);

            List<string> lines = File.ReadAllLines("Data/Day8Input.txt").ToList();
            int sum = 0;
            foreach (var line in lines)
            {
                Console.WriteLine("Line");
                sum += LineValue(line, possibleValues);

              
            }
            return sum;
        }

        private static int LineValue(string line, Dictionary<string, int> possibleValues)
        {
            int sum = 0;
            var parts = line.Split('|');

            for (int a = 1; a < 9; ++a)
            {
                for (int b = 1; b < 9; ++b)
                {
                    for (int c = 1; c < 9; ++c)
                    {
                        for (int d = 1; d < 9; ++d)
                        {
                            for (int e = 1; e < 9; ++e)
                            {
                                for (int f = 1; f < 9; ++f)
                                {
                                    for (int g = 1; g < 9; ++g)
                                    {
                                        bool isPossible = true;
                                        var temp = parts[0];
                                        temp = temp.Replace('a', a.ToString()[0]);
                                        temp = temp.Replace('b', b.ToString()[0]);
                                        temp = temp.Replace('c', c.ToString()[0]);
                                        temp = temp.Replace('d', d.ToString()[0]);
                                        temp = temp.Replace('e', e.ToString()[0]);
                                        temp = temp.Replace('f', f.ToString()[0]);
                                        temp = temp.Replace('g', g.ToString()[0]);

                                        var individuals = temp.Split(' ');
                                        foreach (var item in individuals.Where(x => !string.IsNullOrEmpty(x)))
                                        {
                                            var foo = item.OrderBy(x => int.Parse(x.ToString()));
                                            string asStr = "";

                                            foreach (var car in foo)
                                            {
                                                asStr += car;
                                            }
                                            if (!possibleValues.ContainsKey(asStr))
                                            {
                                                isPossible = false;
                                            }
                                        }
                                        if (isPossible)
                                        {
                                            temp = parts[1];
                                            temp = temp.Replace('a', a.ToString()[0]);
                                            temp = temp.Replace('b', b.ToString()[0]);
                                            temp = temp.Replace('c', c.ToString()[0]);
                                            temp = temp.Replace('d', d.ToString()[0]);
                                            temp = temp.Replace('e', e.ToString()[0]);
                                            temp = temp.Replace('f', f.ToString()[0]);
                                            temp = temp.Replace('g', g.ToString()[0]);

                                            individuals = temp.Split(' ');
                                            int multipler = 1000;
                                            foreach (var item in individuals.Where(x => !string.IsNullOrEmpty(x)))
                                            {
                                                var foo = item.OrderBy(x => int.Parse(x.ToString()));
                                                string asStr = "";

                                                foreach (var car in foo)
                                                {
                                                    asStr += car;
                                                }
                                                sum += multipler * possibleValues[asStr];
                                                multipler /= 10;
                                            }

                                            return sum;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            throw new Exception();
        }
    }
}