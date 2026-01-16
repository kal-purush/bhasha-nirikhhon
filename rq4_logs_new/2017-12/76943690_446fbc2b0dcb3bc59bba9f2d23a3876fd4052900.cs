using System;
using System.Collections.Generic;
using System.Linq;

namespace Advent2017.Solutions
{
    class Day13
    {
        public int CalculateSeverity(string[] sample)
        {
            var layers = GetLayers(sample);
            return layers.Where(layer => layer.Item1 % (2 * (layer.Item2 - 1)) == 0)
                .Sum(layer => layer.Item1 * layer.Item2);
        }

        public int Severity(IEnumerable<Tuple<int,int>> layers)
        {
            return layers.Where(layer => layer.Item1 % (2 * (layer.Item2 - 1)) == 0)
                .Sum(layer => layer.Item1 * layer.Item2);
        }

        private IEnumerable<Tuple<int, int>> GetLayers(string[] sample)
        {
            return sample.Select(x => new Tuple<int, int>
            (
                Convert.ToInt32(x.Split(':')[0]), //item1 = Range
                Convert.ToInt32(x.Split(':')[1]) //item 2 = Depth
            ));
        }

        public int CalculateDelay(string[] sample)
        {
            var layers = GetLayers(sample);

            return Enumerable.Range(0, int.MaxValue).First(d => 
                layers.All(s => (s.Item1 + d) % (2 * (s.Item2 - 1)) != 0));
        }
    }
}