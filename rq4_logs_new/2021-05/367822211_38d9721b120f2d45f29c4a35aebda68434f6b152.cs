using System;
using Commivoyajer_Core.Models;

namespace Commivoyager.DyncamicProgramming
{
    public static class PathFinder
    {
        public static Output FindTheWay(double[,] ways)
        {
            var dotsCount = ways.GetLength(0);
            var temp = new double[1 << dotsCount, dotsCount];

            for (int i = 0; i < temp.GetLength(0); i++)
            {
                for (int j = 0; j < temp.GetLength(1); j++)
                {
                    temp[i, j] = double.PositiveInfinity;
                }
            }

            temp[0, 0] = 0.0;

            for (var mask = 0L; mask < (1 << dotsCount); mask++)
            {
                for (int i = 0; i < dotsCount; i++)
                {
                    if(temp[mask, i] == double.PositiveInfinity)
                    {
                        continue;
                    }

                    for (int j = 0; j < dotsCount; j++)
                    {
                        if ((mask & (1 << j)) == 0)
                        {
                            temp[mask ^ (1 << j), j] = Math.Min(temp[mask ^ (1 << j), j], temp[mask, i] + ways[i, j]);
                        }
                    }
                }
            }

            var resultPathLength = temp[(1 << dotsCount) - 1, 0];

            var backTrackLength = resultPathLength;
            var pathTrace = new int[dotsCount];
            var pathMask = (1 << dotsCount) - 1;
            var currentPathMask = (1 << dotsCount) - 1;
            var currentCity = 0;

            for (int i = 0; i < dotsCount; i++)
            {
                var pathSegmentLength = temp[currentPathMask, currentCity];
                currentPathMask ^= (1 << currentCity);

                for (int j = 0; j < dotsCount; j++)
                {
                    if ((currentPathMask & (1 << j)) != 0 && temp[currentPathMask, j] + ways[j, currentCity] - pathSegmentLength < 0.0001)
                    {
                        pathTrace[i] = j;
                        currentCity = j;
                    }
                }
            }

            return new Output
            {
                Sequence = pathTrace,
                JourneyLength = temp[(1 << dotsCount) - 1, 0],
                VariantsCount = 1
            };
        }
    }
}