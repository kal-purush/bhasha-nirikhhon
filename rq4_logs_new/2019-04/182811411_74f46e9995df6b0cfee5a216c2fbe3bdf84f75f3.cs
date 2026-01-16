using Infrastructor.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructor
{
    public class Finder
    {
        public List<HotelTilePoint> Find(int xCurrent,int yCurrent,int count, int xSize, int ySize, int xTileSize, int yTileSize, Dictionary<int,List<HotelTilePoint>> data)
        {
            List<HotelTilePoint> result = new List<HotelTilePoint>();

            TileIdClaculator tileIdGenerator = new TileIdClaculator();

            int tileId = tileIdGenerator.GenerateTileId(xSize, ySize, xTileSize, yTileSize, xCurrent, yCurrent);

            result.AddRange(data[tileId]);

            foreach (int nearId in tileIdGenerator.findNears(tileId, xSize, ySize, xTileSize, yTileSize))
            {
                result.AddRange(data[nearId]);
            }

            result = result.Take(count).ToList();
      
            return result;
        }

        public int[] findNears()
        {

            return null;
        }


        public int CalcDistance(int x1,int y1,int x2,int y2)
        {
            return  (int)Math.Abs( Math.Pow(x1 - x2, 2) + Math.Pow(y1 - y2, 2));
        }
    }
}