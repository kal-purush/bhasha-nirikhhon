using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace mst_boredom_remover
{
    class EngineMap
    {
        public int width;
        public int height;

        public Tile[,] tiles;

        public enum Directions
        {
            North,
            South,
            East,
            West
        }

        public EngineMap(int width, int height)
        {
            this.width = width;
            this.height = height;
            this.tiles = new Tile[width, height];
        }
    }
}