using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Advent2017.Solutions
{
    class Day11
    {
        
        //Thank you https://www.redblobgames.com/grids/hexagons/#distances
        public int Distance(string directions, bool part2 = false)
        {
            int x=0, y=0, z = 0;
            int furthestDistance = 0;
            foreach (var direction in directions.Split(','))
            {
                switch (direction)
                {
                    case "n":
                        y++;
                        z--;
                        break;
                    case "ne":
                        x++;
                        z--;
                        break;
                    case "se":
                        y--;
                        x++;
                        break;
                    case "s":
                        y--;
                        z++;
                        break;
                    case "sw":
                        x--;
                        z++;
                        break;
                    case "nw":
                        x--;
                        y++;
                        break;
                }
                if (CubicDistance(x, y, z) > furthestDistance)
                    furthestDistance = CubicDistance(x, y, z);
            }

            return part2 ? furthestDistance : CubicDistance(x, y, z);
            
        }

        private static int CubicDistance(int x, int y, int z)
        {
            return (Math.Abs(x - 0) + Math.Abs(y - 0) + Math.Abs(z - 0)) / 2;
        }
    }
}