using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using drillGame;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;


namespace TerrainGeneration
{
    public class TerrainGenerator
    {
        List<Tile> tiles = new List<Tile>();
        private int seed;
        private int width, height;
        Texture2D grassTile, sandTile, waterTile;

        public void Generate()
        {
            tiles = new List<Tile>();
            seed = GenerateSeed();
            for (int x = 0; x < width; x++)
            {
                for (int y = 0; y < height; y++)
                {
                    float value = 1; //Noise.Generate((x / width) * seed, (y / height) * seed) / 10.0f;
                    if (value <= 0.1f)
                    {
                        tiles.Add(new Tile(new Vector2(x * y).ToInt() * Tile.size), grassTile);
                    }
                    else if (value > 0.1f && value <= 0.5f)
                    {
                        tiles.Add(new Tile(new Vector2(x * y).ToInt() * Tile.size), sandTile);
                    }
                    else
                    {
                        tiles.Add(new Tile(new Vector2(x * y).ToInt() * Tile.size), waterTile);
                    }
                }
            }
        }

        public int GenerateSeed()
        {
            Random random = new Random();
            int length = 8;
            int result = 0;

            for (int i = 0; i < length; i++)
            {
                result += random.Next(0, 9);
            }

            return result;
        }
    }
}