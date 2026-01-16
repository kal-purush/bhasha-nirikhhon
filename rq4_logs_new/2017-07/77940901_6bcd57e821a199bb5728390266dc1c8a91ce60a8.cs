using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Implementations
    {
    public class FlatLandSpaceStation
        {
        public static void Main (string[] args)
            {
            string[] limits = Console.ReadLine().Split(' ');
            string[] stringArr = Console.ReadLine().Split(' ');
            int noOfCities = Convert.ToInt32(limits[0]);
            int spaceStations = Convert.ToInt32(limits[1]);
            bool[] cities = new bool[noOfCities];
            int[] indexSpaceStation = Array.ConvertAll(stringArr, Int32.Parse);
            int maxDist = 0;
            for (int i = 0; i < spaceStations - 1; i++)
            {
                int start = indexSpaceStation[i] + 1;
                int end = indexSpaceStation[i + 1] - 1;
                int mid = (start + end)/2;

                if (maxDist < mid)
                    maxDist = mid;
            }
            Console.WriteLine(maxDist);
            Console.ReadKey();
            }
        }
    }