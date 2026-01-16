using Commivoyajer_Core.Models;
using System.Collections.Generic;

namespace Commivoyajer_Framework.Methods
{
    public class GreedyMethod
    {
        public Output GetGreedyMethod(double[,] input)
        {
            var output = new List<Output>();

            for (int i = 0; i < input.GetLength(0); i++)
            {
                for (int j = 0; j < input.GetLength(0); j++)
                {
                    var sequence = new int[input.GetLength(0)];
                    var cityIndex = i;
                    sequence[j] = cityIndex;



                }
            }

            return output[0];
        }
    }
}