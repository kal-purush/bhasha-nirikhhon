using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AdventOfCode2021
{

    internal class Spawner
    {
        public string Trigger { get; set; }
        public string Increment { get; set; }
        public List<Spawner> Children { get; set; } = new List<Spawner>();
    }


    internal class Day14
    {
        public static long Run()
        {
            List<string> lines = File.ReadAllLines("Data/Day14Input.txt").ToList();

            string inputString = lines[0];

            List<Spawner> defaultSpawners = new List<Spawner>();

            Dictionary<string, string> swaps = new Dictionary<string, string>();
            for (int i = 2; i < lines.Count; i++)
            {
                var bits = lines[i].Split(" -> ");
                defaultSpawners.Add(new Spawner { Trigger = bits[0], Increment = bits[1] });
            }


            foreach (var item in defaultSpawners)
            {
                string child1 = item.Trigger[0] + item.Increment;
                string child2 = item.Increment + item.Trigger[1];
                if (defaultSpawners.Any(x => x.Trigger == child1))
                {
                    item.Children.Add(defaultSpawners.First(x => x.Trigger == child1));
                }
                if (defaultSpawners.Any(x => x.Trigger == child2))
                {
                    item.Children.Add(defaultSpawners.First(x => x.Trigger == child2));
                }
            }

            Dictionary<Spawner, long> currentStep = new Dictionary<Spawner, long>();

            for (int i = 0; i < inputString.Length - 1; i++)
            {
                var spawner = defaultSpawners.First(x => x.Trigger == inputString.Substring(i, 2));
                if (!currentStep.ContainsKey(spawner))
                {
                    currentStep.Add(spawner, 0);
                }
                currentStep[spawner] += 1;
            }

            Dictionary<Char, long> stringValues = new Dictionary<Char, long>();

            foreach (var item in inputString)
            {
                if (!stringValues.ContainsKey(item))
                {
                    stringValues.Add(item, 0);
                }
                stringValues[item] += 1;
            }

            for (int i = 0; i < 40; ++i)
            {
                Dictionary<Spawner, long> nextStep = new Dictionary<Spawner, long>();

                foreach (var item in currentStep)
                {
                    nextStep.Add(item.Key, 0);
                }

                foreach (var item in currentStep)
                {
                    if (!stringValues.ContainsKey(item.Key.Increment[0]))
                    {
                        stringValues.Add(item.Key.Increment[0], 0);
                    }
                    stringValues[item.Key.Increment[0]] += item.Value;


                    foreach (var child in item.Key.Children)
                    {
                        if (!nextStep.ContainsKey(child))
                        {
                            nextStep.Add(child, 0);
                        }
                        nextStep[child] += item.Value;
                    }
                }
                currentStep = nextStep;
            }
            return stringValues.Max(x => x.Value) - stringValues.Min(x => x.Value);
        }
    }
}