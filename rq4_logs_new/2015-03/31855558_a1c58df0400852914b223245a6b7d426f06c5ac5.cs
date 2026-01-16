using System;
using Replify;
using System.Collections.Generic;

namespace CubeDimensions.Repl
{
    public class BigCubeDimensions
    {
        private readonly CubeDimensions2 cubeDimensions;

        public BigCubeDimensions()
        {
            this.cubeDimensions = CreateDimensions();
        }

        private CubeDimensions2 CreateDimensions()
        {
            var c = new CubeDimensions2();

            Dictionary<string, List<DimensionValue>> dimensions = new Dictionary<string, List<DimensionValue>>();


            for (var i = 0; i < 5; i++)
            {
                var dimensionCode = "Dimension" + i;
                dimensions.Add(dimensionCode, new List<DimensionValue>());

                for (var j = 0; j < 1000; j++)
                {
                    dimensions[dimensionCode].Add(new DimensionValue(dimensionCode, dimensionCode + "_" + j));
                }
            }

            var rand = new Random();
            for (var i = 0; i < 1000000; i++)
            {
                var point = new List<DimensionValue>();

                foreach (var dimension in dimensions.Values)
                {
                    var index = rand.Next(dimension.Count);
                    point.Add(dimension[index]);
                }

                c.AddPoint(point);
            }

            return c;
        }
    }

    public class Program
    {
        public static void Main()
        {
            var repl = new ClearScriptRepl();
            repl.AddHostType("BigDimensions", typeof(BigCubeDimensions));

            repl.StartReplLoop();
        }
    }
}